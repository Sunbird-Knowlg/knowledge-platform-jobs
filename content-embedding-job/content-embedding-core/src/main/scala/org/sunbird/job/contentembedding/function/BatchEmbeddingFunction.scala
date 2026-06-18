package org.sunbird.job.contentembedding.function

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation, Types}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.contentembedding.domain.{ChunkedEvent, EmbeddedChunk, EmbeddedEvent}
import org.sunbird.job.contentembedding.factory.EmbeddingServiceFactory
import org.sunbird.job.contentembedding.task.ContentEmbeddingConfig
import org.sunbird.job.util.ScalaJsonUtil
import org.sunbird.job.{BaseProcessKeyedFunction, Metrics}

import scala.collection.JavaConverters._

/**
 * Stage 3 of the content embedding pipeline — batched variant.
 *
 * Buffers [[ChunkedEvent]]s per key slot and flushes them as a single batched
 * API call to the configured embedding service (OpenAI / Azure OpenAI / E5).
 *
 * Flush is triggered by whichever comes first:
 *  - Buffer reaches `embedding.batch_events` events.
 *  - Processing-time timer fires after `embedding.window_size_ms` from the first
 *    event in the current buffer.
 *
 * All chunks from all buffered events are concatenated, then split into sub-batches
 * of `embedding.batch_size` texts each — one `embedBatch` call per sub-batch.
 * Results are concatenated in order and redistributed back to per-event [[EmbeddedEvent]]
 * side outputs. API calls reduced from N (one per event) to ceil(totalChunks / batch_size).
 *
 * Keyed by `Math.abs(objectId.hashCode) % embeddingParallelism` so parallelism
 * is preserved — unlike `windowAll` which forces parallelism = 1.
 *
 * On batch failure every buffered event is individually routed to the DLQ.
 */
class BatchEmbeddingFunction(config: ContentEmbeddingConfig)(implicit stringTypeInfo: TypeInformation[String])
  extends BaseProcessKeyedFunction[Int, ChunkedEvent, String](config) {

  private[this] val logger = LoggerFactory.getLogger(classOf[BatchEmbeddingFunction])

  @transient private var embeddingService: org.sunbird.job.contentembedding.service.EmbeddingService = _
  @transient private var bufferState: ListState[ChunkedEvent] = _
  @transient private var pendingTimer: ValueState[java.lang.Long] = _

  override def open(parameters: Configuration, metrics: Metrics): Unit = {
    embeddingService = EmbeddingServiceFactory.getService(config.embeddingServiceConfig)
    logger.info(s"BatchEmbeddingFunction ready: service=${embeddingService.getName} " +
      s"batchEvents=${config.embeddingBatchEvents} windowMs=${config.embeddingWindowSizeMs}")

    bufferState = getRuntimeContext.getListState(
      new ListStateDescriptor[ChunkedEvent]("embedding-buffer", TypeInformation.of(new TypeHint[ChunkedEvent]() {}))
    )
    pendingTimer = getRuntimeContext.getState(
      new ValueStateDescriptor[java.lang.Long]("embedding-pending-timer", Types.LONG)
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

  override def processElement(
      event: ChunkedEvent,
      context: KeyedProcessFunction[Int, ChunkedEvent, String]#Context,
      metrics: Metrics
  ): Unit = {
    val buffer = bufferState.get().asScala.toList
    val isFirstInBuffer = buffer.isEmpty

    bufferState.add(event)

    if (isFirstInBuffer && config.embeddingWindowSizeMs > 0) {
      val flushAt = context.timerService().currentProcessingTime() + config.embeddingWindowSizeMs
      context.timerService().registerProcessingTimeTimer(flushAt)
      pendingTimer.update(flushAt)
    }

    val updatedSize = buffer.size + 1
    if (updatedSize >= config.embeddingBatchEvents) {
      logger.debug(s"Embedding batch size threshold reached ($updatedSize events), flushing")
      cancelPendingTimer(context.timerService())
      flush(context, metrics)
    }
  }

  override def onTimer(
      timestamp: Long,
      ctx: KeyedProcessFunction[Int, ChunkedEvent, String]#OnTimerContext,
      metrics: Metrics
  ): Unit = {
    val registered = pendingTimer.value()
    if (registered != null && registered == timestamp) {
      val buffer = bufferState.get().asScala.toList
      if (buffer.nonEmpty) {
        logger.debug(s"Embedding window timer fired, flushing ${buffer.size} buffered events")
        pendingTimer.clear()
        flush(ctx, metrics)
      }
    }
  }

  private def cancelPendingTimer(timerService: org.apache.flink.streaming.api.TimerService): Unit = {
    val ts = pendingTimer.value()
    if (ts != null) {
      timerService.deleteProcessingTimeTimer(ts)
      pendingTimer.clear()
    }
  }

  private def flush(
      context: KeyedProcessFunction[Int, ChunkedEvent, String]#Context,
      metrics: Metrics
  ): Unit = {
    val events = bufferState.get().asScala.toList
    bufferState.clear()

    if (events.isEmpty) return

    try {
      // Flatten all chunks across all buffered events; track per-event chunk counts
      // so we can re-split the flat vector list after embedding.
      val flatTexts: List[String] = events.flatMap(_.chunks.map(_.text))
      val chunkCounts: List[Int]  = events.map(_.chunks.size)

      // Sub-batch by embeddingBatchSize to respect the API's input limit (OpenAI: 2048).
      // Each sub-batch is one HTTP call; results are concatenated in order.
      val startNanos = System.nanoTime()
      val allVectors = flatTexts
        .grouped(config.embeddingBatchSize)
        .flatMap(embeddingService.embedBatch)
        .toList
      val elapsedMs  = (System.nanoTime() - startNanos) / 1000000L

      val apiCalls = math.ceil(flatTexts.size.toDouble / config.embeddingBatchSize).toInt
      (1 to apiCalls).foreach(_ => metrics.incCounter(config.embeddingApiCallCount))
      if (elapsedMs > config.embeddingSlowCallThresholdMs) {
        metrics.incCounter(config.embeddingSlowCallCount)
        logger.warn(s"Slow embedding batch: ${elapsedMs}ms for ${events.size} events / ${flatTexts.size} texts / $apiCalls API calls")
      }
      logger.info(s"Embedded batch: ${events.size} events, ${flatTexts.size} chunks, $apiCalls API calls in ${elapsedMs}ms")

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
        logger.error(s"Batch embedding failed for ${events.size} events: ${e.getMessage}", e)
        events.foreach { event =>
          context.output(config.errorOutTag, ScalaJsonUtil.serialize(
            Map("objectId" -> event.objectId, "stage" -> "embedding", "error" -> e.getMessage)
          ))
          metrics.incCounter(config.failedEventCount)
        }
    }
  }
}
