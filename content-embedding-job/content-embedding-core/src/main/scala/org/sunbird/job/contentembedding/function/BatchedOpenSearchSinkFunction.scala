package org.sunbird.job.contentembedding.function

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation, Types}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.contentembedding.domain.EmbeddingOutput
import org.sunbird.job.contentembedding.task.ContentEmbeddingConfig
import org.sunbird.job.util.{ElasticSearchUtil, ScalaJsonUtil}
import org.sunbird.job.{BaseProcessKeyedFunction, Metrics}

import scala.collection.JavaConverters._

/**
 * Stage 5 (terminal) of the content embedding pipeline — batched variant.
 *
 * Buffers [[EmbeddingOutput]] documents and flushes them to OpenSearch as a
 * single bulk update request, replacing the per-document `updateDocumentWithRefresh`
 * call in [[OpenSearchSinkFunction]].
 *
 * Flush is triggered by whichever comes first:
 *  - Buffer reaches `opensearch.bulk.size` documents.
 *  - Processing-time timer fires after `opensearch.bulk.flush_interval_ms`.
 *
 * Partial bulk failures are handled per-item: succeeded documents emit to
 * `successOutTag`; failed documents are individually routed to the DLQ (`errorOutTag`).
 *
 * Keyed by constant `0` — sink parallelism is 1, keying is used only to
 * gain access to Flink managed state and processing-time timers via
 * [[BaseProcessKeyedFunction]].
 */
class BatchedOpenSearchSinkFunction(config: ContentEmbeddingConfig)(implicit stringTypeInfo: TypeInformation[String])
  extends BaseProcessKeyedFunction[Int, EmbeddingOutput, String](config) {

  private[this] val logger = LoggerFactory.getLogger(classOf[BatchedOpenSearchSinkFunction])
  private var esUtil: ElasticSearchUtil = _

  @transient private var bufferState: ListState[EmbeddingOutput] = _
  @transient private var bufferCount: ValueState[java.lang.Integer] = _
  @transient private var pendingTimer: ValueState[java.lang.Long] = _

  override def open(parameters: Configuration, metrics: Metrics): Unit = {
    val connectionInfo = s"${config.openSearchHost}:${config.openSearchPort}"
    esUtil = new ElasticSearchUtil(connectionInfo, config.openSearchIndexName)
    logger.info(s"BatchedOpenSearchSinkFunction ready: $connectionInfo index=${config.openSearchIndexName} " +
      s"bulkSize=${config.osBulkSize} flushIntervalMs=${config.osBulkFlushIntervalMs}")

    bufferState = getRuntimeContext.getListState(
      new ListStateDescriptor[EmbeddingOutput]("opensearch-sink-buffer", TypeInformation.of(new TypeHint[EmbeddingOutput]() {}))
    )
    bufferCount = getRuntimeContext.getState(
      new ValueStateDescriptor[java.lang.Integer]("opensearch-buffer-count", Types.INT)
    )
    pendingTimer = getRuntimeContext.getState(
      new ValueStateDescriptor[java.lang.Long]("opensearch-pending-timer", Types.LONG)
    )
  }

  override def close(): Unit = {
    super.close()
    if (esUtil != null) esUtil.close()
  }

  override def metricsList(): List[String] = List(config.successEventCount, config.failedEventCount)

  override def processElement(
      output: EmbeddingOutput,
      context: KeyedProcessFunction[Int, EmbeddingOutput, String]#Context,
      metrics: Metrics
  ): Unit = {
    val count = Option(bufferCount.value()).map(_.intValue()).getOrElse(0)
    val isFirstInBuffer = count == 0

    bufferState.add(output)
    bufferCount.update(count + 1)

    if (isFirstInBuffer && config.osBulkFlushIntervalMs > 0) {
      val flushAt = context.timerService().currentProcessingTime() + config.osBulkFlushIntervalMs
      context.timerService().registerProcessingTimeTimer(flushAt)
      pendingTimer.update(flushAt)
    }

    if (count + 1 >= config.osBulkSize) {
      logger.debug(s"OpenSearch bulk size threshold reached ($updatedSize docs), flushing")
      cancelPendingTimer(context.timerService())
      flush(context, metrics)
    }
  }

  override def onTimer(
      timestamp: Long,
      ctx: KeyedProcessFunction[Int, EmbeddingOutput, String]#OnTimerContext,
      metrics: Metrics
  ): Unit = {
    val registered = pendingTimer.value()
    if (registered != null && registered == timestamp) {
      val buffer = bufferState.get().asScala.toList
      if (buffer.nonEmpty) {
        logger.debug(s"OpenSearch flush timer fired, flushing ${buffer.size} buffered docs")
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
      context: KeyedProcessFunction[Int, EmbeddingOutput, String]#Context,
      metrics: Metrics
  ): Unit = {
    val outputs = bufferState.get().asScala.toList
    bufferState.clear()
    bufferCount.clear()

    if (outputs.isEmpty) return

    // Deduplicate by objectId (last-wins) before building the bulk request.
    // Without this, .toMap silently drops earlier versions while the loop below
    // would still emit successOutTag for every duplicate — 1 write, N successes.
    val deduped = outputs.groupBy(_.objectId).values.map(_.last).toList
    if (deduped.size < outputs.size)
      logger.warn(s"Deduplicated ${outputs.size - deduped.size} duplicate objectId(s) before bulk flush")

    val docs: Map[String, String] = deduped.map(o => o.objectId -> buildChunksDocument(o)).toMap

    try {
      val failures: Map[String, Exception] = esUtil.bulkUpdateWithRefresh(docs)

      deduped.foreach { output =>
        failures.get(output.objectId) match {
          case Some(ex) =>
            logger.error(s"OpenSearch bulk update failed for ${output.objectId}: ${ex.getMessage}")
            context.output(config.errorOutTag, ScalaJsonUtil.serialize(
              Map("objectId" -> output.objectId, "stage" -> "opensearch", "error" -> ex.getMessage)
            ))
            metrics.incCounter(config.failedEventCount)
          case None =>
            logger.debug(s"Updated chunks for ${output.objectId} (${output.chunks.size} chunks)")
            context.output(config.successOutTag, output.objectId)
            metrics.incCounter(config.successEventCount)
        }
      }
      logger.info(s"OpenSearch bulk flush: ${deduped.size} docs, ${failures.size} failures")
    } catch {
      case e: Exception =>
        logger.error(s"OpenSearch bulk flush failed entirely for ${deduped.size} docs: ${e.getMessage}", e)
        deduped.foreach { output =>
          context.output(config.errorOutTag, ScalaJsonUtil.serialize(
            Map("objectId" -> output.objectId, "stage" -> "opensearch", "error" -> e.getMessage)
          ))
          metrics.incCounter(config.failedEventCount)
        }
    }
  }

  private def buildChunksDocument(output: EmbeddingOutput): String = {
    val chunksData = output.chunks.map { chunk =>
      Map(
        "text"           -> chunk.text,
        "embedding"      -> chunk.embedding.map(_.toInt).toList,
        "word_count"     -> chunk.wordCount,
        "chunk_index"    -> chunk.index,
        "schema_version" -> output.schemaVersion
      )
    }
    ScalaJsonUtil.serialize(Map("chunks" -> chunksData))
  }
}
