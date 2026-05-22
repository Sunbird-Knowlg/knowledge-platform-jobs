package org.sunbird.job.contentembedding.function

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.contentembedding.domain.EnrichedMetadataEvent
import org.sunbird.job.contentembedding.task.ContentEmbeddingConfig
import org.sunbird.job.util.ScalaJsonUtil
import org.sunbird.job.{BaseProcessFunction, Metrics}

/**
 * Stage 1 of the content embedding pipeline.
 *
 * Deserializes raw JSON strings from the `enriched.content.metadata` Kafka topic
 * into typed [[EnrichedMetadataEvent]] objects and routes them to the next stage
 * via the `enrichedOutTag` side output.
 *
 * Errors are emitted to the `errorOutTag` side output and counted in metrics.
 */
class ExtractFunction(config: ContentEmbeddingConfig)(implicit stringTypeInfo: TypeInformation[String])
  extends BaseProcessFunction[String, String](config) {

  private[this] val logger = LoggerFactory.getLogger(classOf[ExtractFunction])

  override def metricsList(): List[String] = List(config.extractedEventsCount, config.failedEventCount)

  override def processElement(
      event: String,
      context: ProcessFunction[String, String]#Context,
      metrics: Metrics
  ): Unit = {
    try {
      val eventMap = ScalaJsonUtil.deserialize[Map[String, Any]](event)

      val enrichedEvent = EnrichedMetadataEvent(
        id = eventMap.get("id").map(_.toString).getOrElse(""),
        contentType = eventMap.get("contentType").map(_.toString).getOrElse("Content"),
        _schema_version = eventMap.get("_schema_version").map(_.toString).getOrElse("1.0"),
        timestamp = eventMap.get("timestamp").map {
          case l: Long => l
          case i: Int  => i.toLong
          case n       => n.toString.toLong
        }.getOrElse(System.currentTimeMillis()),
        data = eventMap.get("data").map(_.asInstanceOf[Map[String, Any]]).getOrElse(Map())
      )

      logger.info(s"Extracted enriched metadata event: ${enrichedEvent.id} (${enrichedEvent.contentType})")
      metrics.incCounter(config.extractedEventsCount)
      context.output(config.enrichedOutTag, enrichedEvent)

    } catch {
      case e: Exception =>
        logger.error(s"Error extracting enriched metadata event: ${e.getMessage}", e)
        context.output(config.errorOutTag, s"Error extracting event: ${e.getMessage}")
        metrics.incCounter(config.failedEventCount)
    }
  }
}
