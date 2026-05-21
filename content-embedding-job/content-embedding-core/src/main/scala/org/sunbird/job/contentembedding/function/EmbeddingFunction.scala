package org.sunbird.job.contentembedding.function

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.contentembedding.domain.{ChunkedEvent, EmbeddedChunk, EmbeddedEvent}
import org.sunbird.job.contentembedding.factory.EmbeddingServiceFactory
import org.sunbird.job.contentembedding.task.ContentEmbeddingConfig
import org.sunbird.job.{BaseProcessFunction, Metrics}

class EmbeddingFunction(config: ContentEmbeddingConfig)(implicit stringTypeInfo: TypeInformation[String])
  extends BaseProcessFunction[ChunkedEvent, String](config) {

  private[this] val logger = LoggerFactory.getLogger(classOf[EmbeddingFunction])
  @transient private var embeddingService: org.sunbird.job.contentembedding.service.EmbeddingService = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    embeddingService = EmbeddingServiceFactory.getService(config.embeddingServiceConfig)
    logger.info(s"Initialized EmbeddingService: ${embeddingService.getName} v${embeddingService.getVersion} (${embeddingService.getDimensions}d)")
  }

  override def close(): Unit = {
    super.close()
    embeddingService.close()
  }

  override def metricsList(): List[String] = List(config.embeddedEventsCount, config.failedEventCount)

  override def processElement(
      event: ChunkedEvent,
      context: ProcessFunction[ChunkedEvent, String]#Context,
      metrics: Metrics
  ): Unit = {
    try {
      val texts = event.chunks.map(_.text)
      val vectors = embeddingService.embedBatch(texts)

      val embeddedChunks = event.chunks.zip(vectors).map { case (chunk, vector) =>
        EmbeddedChunk(
          text = chunk.text,
          sourceField = chunk.sourceField,
          chunkIndex = chunk.index,
          vector = vector,
          tokenCount = chunk.text.split("\\s+").length,
          modelId = embeddingService.getName
        )
      }

      logger.info(s"Embedded ${embeddedChunks.size} chunks for ${event.objectId}")
      metrics.incCounter(config.embeddedEventsCount)
      context.output(config.embeddedOutTag, EmbeddedEvent(event.objectId, event.contentType, event.schemaVersion, embeddedChunks))
    } catch {
      case e: Exception =>
        logger.error(s"Error embedding ${event.objectId}: ${e.getMessage}", e)
        context.output(config.errorOutTag, s"""{"objectId":"${event.objectId}","stage":"embedding","error":"${e.getMessage}"}""")
        metrics.incCounter(config.failedEventCount)
    }
  }
}
