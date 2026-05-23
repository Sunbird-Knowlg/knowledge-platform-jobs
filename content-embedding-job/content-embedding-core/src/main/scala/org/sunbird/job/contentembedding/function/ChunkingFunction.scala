package org.sunbird.job.contentembedding.function

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.contentembedding.domain.{ChunkedEvent, EnrichedMetadataEvent}
import org.sunbird.job.contentembedding.factory.ChunkingStrategyFactory
import org.sunbird.job.contentembedding.task.ContentEmbeddingConfig
import org.sunbird.job.{BaseProcessFunction, Metrics}

/**
 * Stage 2 of the content embedding pipeline.
 *
 * Splits an [[EnrichedMetadataEvent]] into a list of [[org.sunbird.job.contentembedding.domain.TextChunk]]s
 * using the configured [[org.sunbird.job.contentembedding.service.ChunkingStrategy]]
 * (`"semantic"` or `"sliding-window"`).
 *
 * Events with no usable text (empty chunk list) are filtered and counted as `filteredEventsCount`.
 * Chunked events are emitted via the `chunkedOutTag` side output.
 */
class ChunkingFunction(config: ContentEmbeddingConfig)(implicit stringTypeInfo: TypeInformation[String])
  extends BaseProcessFunction[EnrichedMetadataEvent, String](config) {

  private[this] val logger = LoggerFactory.getLogger(classOf[ChunkingFunction])
  private var chunkingStrategy: org.sunbird.job.contentembedding.service.ChunkingStrategy = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    chunkingStrategy = ChunkingStrategyFactory.getStrategy(config.chunkingStrategyConfig)
    logger.info(s"Initialized ChunkingStrategy: ${chunkingStrategy.getName} v${chunkingStrategy.getVersion}")
  }

  override def metricsList(): List[String] = List(config.chunkedEventsCount, config.filteredEventsCount, config.failedEventCount)

  override def processElement(
      event: EnrichedMetadataEvent,
      context: ProcessFunction[EnrichedMetadataEvent, String]#Context,
      metrics: Metrics
  ): Unit = {
    try {
      logger.debug(s"Chunking event ${event.id} with ${event.data.size} data properties")
      val chunks = chunkingStrategy.chunk(event.id, event.contentType, event.data)

      if (chunks == null) {
        throw new IllegalStateException(s"ChunkingStrategy returned null for event ${event.id}")
      }

      logger.info(s"Generated ${chunks.size} chunks for ${event.id} (${event.contentType})")
      if (chunks.nonEmpty) {
        val totalChars = chunks.map(_.text.length).sum
        logger.debug(s"Chunk details: ${chunks.size} chunks, $totalChars total characters")
        metrics.incCounter(config.chunkedEventsCount)
        context.output(config.chunkedOutTag, ChunkedEvent(event.id, event.contentType, event._schema_version, chunks))
      } else {
        logger.warn(s"No chunks generated for ${event.id} — skipping")
        metrics.incCounter(config.filteredEventsCount)
      }
    } catch {
      case e: Exception =>
        logger.error(s"Error chunking ${event.id}: ${e.getMessage}", e)
        context.output(config.errorOutTag, s"""{"objectId":"${event.id}","stage":"chunking","error":"${e.getMessage}"}""")
        metrics.incCounter(config.failedEventCount)
    }
  }
}
