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
      logger.debug(s"Processing raw event: ${event.take(200)}...")
      val eventMap = ScalaJsonUtil.deserialize[Map[String, Any]](event)
      logger.debug(s"Successfully deserialized event JSON. Keys: ${eventMap.keys.mkString(",")}")

      val id = eventMap.get("id").map(_.toString)
        .filter(_.nonEmpty)
        .getOrElse(throw new IllegalArgumentException("Missing or empty id field"))
      logger.debug(s"Extracted id: $id")

      val contentType = eventMap.get("contentType").map(_.toString).getOrElse("Content")
      logger.debug(s"Content type: $contentType")

      val schemaVersion = eventMap.get("_schema_version").map(_.toString).getOrElse("1.0")
      logger.debug(s"Schema version: $schemaVersion")

      val timestamp = eventMap.get("timestamp").map {
        case l: Long => l
        case i: Int  => i.toLong
        case n       => n.toString.toLong
      }.getOrElse(System.currentTimeMillis())
      logger.debug(s"Timestamp: $timestamp")

      val data: Map[String, Any] = eventMap.get("data") match {
        case Some(m: Map[_, _]) =>
          val dataMap = m.asInstanceOf[Map[String, Any]]
          logger.debug(s"Data map extracted with ${dataMap.size} keys")
          dataMap
        case Some(other) => throw new IllegalArgumentException(s"data field is not a map: ${other.getClass.getSimpleName}")
        case None =>
          logger.debug("No data field found, using empty map")
          Map.empty[String, Any]
      }

      val enrichedEvent = EnrichedMetadataEvent(
        id = id,
        contentType = contentType,
        _schema_version = schemaVersion,
        timestamp = timestamp,
        data = data
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
