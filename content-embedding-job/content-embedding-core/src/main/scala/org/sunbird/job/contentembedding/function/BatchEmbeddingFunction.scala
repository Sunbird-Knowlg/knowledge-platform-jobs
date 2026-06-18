package org.sunbird.job.contentembedding.function

import org.apache.flink.api.common.state.ListStateDescriptor
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.contentembedding.domain.{ChunkedEvent, EmbeddedChunk, EmbeddedEvent}
import org.sunbird.job.contentembedding.factory.EmbeddingServiceFactory
import org.sunbird.job.contentembedding.task.ContentEmbeddingConfig
import org.sunbird.job.util.ScalaJsonUtil
import org.sunbird.job.Metrics

/**
 * Stage 3 of the content embedding pipeline — batched variant.
 *
 * Buffers [[ChunkedEvent]]s into shared key buckets and flushes them as batched
 * API calls to the configured embedding service (OpenAI / Azure OpenAI / E5).
 *
 * Windowing protocol (size + time triggers) is handled by [[BaseBatchingKeyedFunction]].
 * Each flush concatenates all chunks across buffered events, sub-batches them by
 * `embedding.batch_size` (one HTTP request per sub-batch), then redistributes
 * vectors back to per-event [[EmbeddedEvent]] side outputs.
 *
 * Keyed by `Math.abs(objectId.hashCode) % embeddingParallelism` to produce exactly
 * `embeddingParallelism` distinct integer keys. This forces events from different
 * objectIds into shared key buckets so their chunks accumulate in the same buffer —
 * keying by objectId directly would give each objectId its own independent buffer,
 * defeating cross-event batching entirely. Flink maps these key buckets to subtasks
 * via murmur hash (not 1:1 with parallelism), so slot load is approximately but not
 * perfectly uniform.
 *
 * On batch failure each event is retried individually; only the broken event hits DLQ.
 */
class BatchEmbeddingFunction(config: ContentEmbeddingConfig)(implicit stringTypeInfo: TypeInformation[String])
  extends BaseBatchingKeyedFunction[Int, ChunkedEvent, String](config) {

  private[this] val logger = LoggerFactory.getLogger(classOf[BatchEmbeddingFunction])

  @transient private var embeddingService: org.sunbird.job.contentembedding.service.EmbeddingService = _

  override protected def batchSize: Int  = config.embeddingBatchEvents
  override protected def windowMs: Long  = config.embeddingWindowSizeMs

  override def open(parameters: Configuration, metrics: Metrics): Unit = {
    embeddingService = EmbeddingServiceFactory.getService(config.embeddingServiceConfig)
    logger.info(s"BatchEmbeddingFunction ready: service=${embeddingService.getName} " +
      s"batchEvents=${config.embeddingBatchEvents} windowMs=${config.embeddingWindowSizeMs}")
    initWindowState(
      new ListStateDescriptor[ChunkedEvent]("embedding-buffer", TypeInformation.of(new TypeHint[ChunkedEvent]() {})),
      "embedding"
    )
  }

  override def close(): Unit = {
    super.close()
    if (embeddingService != null) embeddingService.close()
  }

  override def metricsList(): List[String] = List(
    config.embeddedEventsCount,
    config.failedEventCount,
    config.embeddingApiCallCount,
    config.embeddingSlowCallCount
  )

  override protected def doFlush(
      events: List[ChunkedEvent],
      context: KeyedProcessFunction[Int, ChunkedEvent, String]#Context,
      metrics: Metrics
  ): Unit = {
    val flatTexts: List[String] = events.flatMap(_.chunks.map(_.text))
    val chunkCounts: List[Int]  = events.map(_.chunks.size)

    try {
      // Sub-batch by embeddingBatchSize to respect the API's input limit (OpenAI: 2048).
      // Each sub-batch is one HTTP call; results are concatenated in order.
      val startNanos = System.nanoTime()
      val allVectors = flatTexts
        .grouped(config.embeddingBatchSize)
        .flatMap(embeddingService.embedBatch)
        .toList
      val elapsedMs = (System.nanoTime() - startNanos) / 1000000L

      val apiCalls = math.ceil(flatTexts.size.toDouble / config.embeddingBatchSize).toInt
      (1 to apiCalls).foreach(_ => metrics.incCounter(config.embeddingApiCallCount))
      if (elapsedMs > config.embeddingSlowCallThresholdMs) {
        metrics.incCounter(config.embeddingSlowCallCount)
        logger.warn(s"Slow embedding batch: ${elapsedMs}ms for ${events.size} events / ${flatTexts.size} texts / $apiCalls API calls")
      }
      logger.info(s"Embedded batch: ${events.size} events, ${flatTexts.size} chunks, $apiCalls API calls in ${elapsedMs}ms")

      require(allVectors.size == flatTexts.size,
        s"embedBatch returned ${allVectors.size} vectors for ${flatTexts.size} texts — cannot split safely")

      // Re-split flat vector list back to per-event slices using chunkCounts.
      var offset = 0
      events.zip(chunkCounts).foreach { case (event, count) =>
        val vectors = allVectors.slice(offset, offset + count)
        offset += count
        val embeddedChunks = event.chunks.zip(vectors).map { case (chunk, vector) =>
          EmbeddedChunk(
            text        = chunk.text,
            sourceField = chunk.sourceField,
            chunkIndex  = chunk.index,
            vector      = vector,
            wordCount   = chunk.text.split("\\s+").length,
            modelId     = embeddingService.getName
          )
        }
        metrics.incCounter(config.embeddedEventsCount)
        context.output(
          config.embeddedOutTag,
          EmbeddedEvent(event.objectId, event.contentType, event.schemaVersion, embeddedChunks)
        )
      }
    } catch {
      case e: Exception =>
        logger.warn(s"Batch embedding failed for ${events.size} events, retrying individually: ${e.getMessage}")
        events.foreach { event =>
          try {
            val vectors = event.chunks.map(_.text)
              .grouped(config.embeddingBatchSize)
              .flatMap(embeddingService.embedBatch)
              .toList
            val embeddedChunks = event.chunks.zip(vectors).map { case (chunk, vector) =>
              EmbeddedChunk(
                text        = chunk.text,
                sourceField = chunk.sourceField,
                chunkIndex  = chunk.index,
                vector      = vector,
                wordCount   = chunk.text.split("\\s+").length,
                modelId     = embeddingService.getName
              )
            }
            metrics.incCounter(config.embeddedEventsCount)
            context.output(
              config.embeddedOutTag,
              EmbeddedEvent(event.objectId, event.contentType, event.schemaVersion, embeddedChunks)
            )
          } catch {
            case ex: Exception =>
              logger.error(s"Embedding failed for ${event.objectId}: ${ex.getMessage}", ex)
              context.output(config.errorOutTag, ScalaJsonUtil.serialize(
                Map("objectId" -> event.objectId, "stage" -> "embedding", "error" -> ex.getMessage)
              ))
              metrics.incCounter(config.failedEventCount)
          }
        }
    }
  }
}
