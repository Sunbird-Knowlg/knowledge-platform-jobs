package org.sunbird.job.knowlg.function

import com.datastax.driver.core.querybuilder.QueryBuilder
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.knowlg.publish.domain.Event
import org.sunbird.job.knowlg.task.KnowlgPublishConfig
import org.sunbird.job.publish.core.{ExtDataConfig, ObjectData}
import org.sunbird.job.publish.helpers.{ConfigurableEnrichedMetadataEventBuilder, FieldConfiguration}
import org.sunbird.job.util.{CassandraUtil, JanusGraphUtil, ScalaJsonUtil}
import org.sunbird.job.{BaseProcessFunction, Metrics}

import scala.collection.JavaConverters._

/**
 * Enrich-only path inside knowlg-publish.
 *
 * Triggered by events with `edata.action = "enrich"` on the existing publish topic.
 * Reads the published node from JanusGraph, fetches hierarchy from Cassandra for
 * Collection/QuestionSet, then emits to `enrichedMetadataEventOutTag` using the same
 * `ConfigurableEnrichedMetadataEventBuilder` as normal publish — same field config, same format.
 */
class EnrichOnlyFunction(config: KnowlgPublishConfig,
                         @transient var janusGraphUtil: JanusGraphUtil = null,
                         @transient var cassandraUtil: CassandraUtil = null)
                        (implicit val stringTypeInfo: TypeInformation[String])
  extends BaseProcessFunction[Event, String](config) {

  private[this] val logger = LoggerFactory.getLogger(classOf[EnrichOnlyFunction])

  private val collectionReaderConfig  = ExtDataConfig(config.hierarchyKeyspaceName,    config.hierarchyTableName)
  private val questionSetReaderConfig = ExtDataConfig(config.questionSetKeyspaceName,   config.questionSetTableName)

  private var fieldConfig: FieldConfiguration = _
  private var enrichedMetadataEventBuilder: ConfigurableEnrichedMetadataEventBuilder = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    janusGraphUtil  = new JanusGraphUtil(config)
    cassandraUtil   = new CassandraUtil(config.cassandraHost, config.cassandraPort, config)
    fieldConfig     = new FieldConfiguration(config.getConfig())
    enrichedMetadataEventBuilder = new ConfigurableEnrichedMetadataEventBuilder(
      fieldConfig, config.enrichedMetadataTopic, config.includeHierarchyInEnrichedMetadata)
    logger.info("EnrichOnlyFunction opened")
  }

  override def close(): Unit = {
    super.close()
    if (cassandraUtil != null) cassandraUtil.close()
  }

  override def metricsList(): List[String] =
    List(config.enrichOnlyEventCount, config.enrichOnlySuccessCount, config.enrichOnlyFailedCount)

  override def processElement(
      event: Event,
      context: ProcessFunction[Event, String]#Context,
      metrics: Metrics
  ): Unit = {
    val objectId = event.identifier
    metrics.incCounter(config.enrichOnlyEventCount)
    logger.info(s"EnrichOnlyFunction: processing $objectId")

    if (!config.enrichedMetadataEnabled) {
      logger.info(s"EnrichOnlyFunction: enriched metadata disabled globally, skipping $objectId")
      metrics.incCounter(config.enrichOnlyFailedCount)
      return
    }

    val isTypeEnabled = event.objectType match {
      case "Content" => config.contentEnrichedMetadataEnabled
      case "Collection" => config.collectionEnrichedMetadataEnabled
      case "Question" => config.questionEnrichedMetadataEnabled
      case "QuestionSet" => config.questionSetEnrichedMetadataEnabled
      case _ => false
    }

    if (!isTypeEnabled) {
      logger.info(s"EnrichOnlyFunction: enriched metadata disabled for type ${event.objectType}, skipping $objectId")
      metrics.incCounter(config.enrichOnlyFailedCount)
      return
    }

    try {
      val nodeProps = Option(janusGraphUtil.getNodeProperties(objectId))
        .getOrElse(throw new RuntimeException(s"Node not found in JanusGraph: $objectId"))

      val rawMeta = nodeProps.asScala.toMap[String, AnyRef]
      val metadata: Map[String, AnyRef] = rawMeta
        .filter { case (k, _) => !k.startsWith("IL_") && !k.startsWith("SYS_") } ++
        Map("identifier" -> objectId, "objectType" -> event.objectType)

      val hierarchyOpt: Option[Map[String, AnyRef]] = if (config.includeHierarchyInEnrichedMetadata) {
        event.objectType match {
          case "Collection" =>
            readHierarchy(objectId, collectionReaderConfig)
          case "QuestionSet" =>
            readHierarchy(objectId, questionSetReaderConfig)
          case _ => None
        }
      } else None

      val obj = new ObjectData(objectId, metadata, None, hierarchyOpt)
      val enrichedEvent = enrichedMetadataEventBuilder.buildEnrichedKafkaEvent(obj)
      val eventJson = ScalaJsonUtil.serialize(enrichedEvent)
      context.output(config.enrichedMetadataEventOutTag, eventJson)
      metrics.incCounter(config.enrichOnlySuccessCount)
      logger.info(s"EnrichOnlyFunction: enriched event emitted for $objectId")
    } catch {
      case e: Exception =>
        logger.error(s"EnrichOnlyFunction: failed for $objectId — ${e.getMessage}", e)
        metrics.incCounter(config.enrichOnlyFailedCount)
        context.output(config.failedEventOutTag,
          s"""{"objectId":"$objectId","stage":"enrich-only","error":"${e.getMessage.replace("\"", "'")}"}""")
    }
  }

  private def readHierarchy(identifier: String, readerConfig: ExtDataConfig): Option[Map[String, AnyRef]] = {
    try {
      val select = QueryBuilder.select().all()
        .from(readerConfig.keyspace, readerConfig.table)
        .where()
        .and(QueryBuilder.eq("identifier", identifier))
      val row = cassandraUtil.findOne(select.toString)
      if (row != null) {
        val hierarchyStr = row.getString("hierarchy")
        if (hierarchyStr != null && hierarchyStr.nonEmpty)
          Some(ScalaJsonUtil.deserialize[Map[String, AnyRef]](hierarchyStr))
        else None
      } else None
    } catch {
      case e: Exception =>
        logger.warn(s"Could not read hierarchy for $identifier: ${e.getMessage}")
        None
    }
  }
}
