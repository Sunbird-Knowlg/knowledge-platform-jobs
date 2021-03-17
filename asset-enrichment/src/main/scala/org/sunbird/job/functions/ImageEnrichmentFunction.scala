package org.sunbird.job.functions

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.domain.Event
import org.sunbird.job.domain.`object`.DefinitionCache
import org.sunbird.job.helpers.{ImageEnrichmentHelper, OptimizerHelper}
import org.sunbird.job.models.Asset
import org.sunbird.job.task.AssetEnrichmentConfig
import org.sunbird.job.util.{CloudStorageUtil, Neo4JUtil}
import org.sunbird.job.{BaseProcessFunction, Metrics}

import scala.collection.JavaConverters._

class ImageEnrichmentFunction(config: AssetEnrichmentConfig,
                              @transient var neo4JUtil: Neo4JUtil = null)
  extends BaseProcessFunction[Event, String](config)
  with ImageEnrichmentHelper with OptimizerHelper {

  private[this] val logger = LoggerFactory.getLogger(classOf[ImageEnrichmentFunction])
  lazy val definitionCache: DefinitionCache = new DefinitionCache
  lazy val cloudStorageUtil: CloudStorageUtil = new CloudStorageUtil(config)

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    neo4JUtil = new Neo4JUtil(config.graphRoutePath, config.graphName)
  }

  override def close(): Unit = {
    super.close()
  }

  override def processElement(event: Event, context: ProcessFunction[Event, String]#Context, metrics: Metrics): Unit = {
    logger.info(s"Received message for Image Enrichment for identifier : ${event.id}.")
    metrics.incCounter(config.imageEnrichmentEventCount)
    val asset = Asset(event.data)
    try {
      if (validateForArtifactUrl(asset, config.contentUploadContextDriven)) replaceArtifactUrl(asset)(cloudStorageUtil)
      asset.setMetaData(getMetaData(event.id)(neo4JUtil))
      imageEnrichment(asset)(config, definitionCache, cloudStorageUtil, neo4JUtil)
      metrics.incCounter(config.successImageEnrichmentEventCount)
    } catch {
      case ex: Exception =>
        logger.error(s"Error while processing message for Image Enrichment for identifier : ${asset.identifier}.", ex)
        metrics.incCounter(config.failedImageEnrichmentEventCount)
        throw ex
    }
  }

  override def metricsList(): List[String] = {
    List(config.successImageEnrichmentEventCount, config.failedImageEnrichmentEventCount, config.imageEnrichmentEventCount)
  }

  def getMetaData(identifier: String)(neo4JUtil: Neo4JUtil): Map[String, AnyRef] = {
    val metaData = neo4JUtil.getNodeProperties(identifier).asScala.toMap
    if(metaData != null && metaData.nonEmpty) metaData else throw new Exception(s"Received null or Empty metaData for identifier: $identifier.")
  }

}