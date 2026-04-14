package org.sunbird.job.assetenricment.functions

import org.apache.commons.lang3.StringUtils
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.assetenricment.domain.Event
import org.sunbird.job.assetenricment.helpers.{ImageEnrichmentHelper, OptimizerHelper}
import org.sunbird.job.assetenricment.models.Asset
import org.sunbird.job.assetenricment.task.AssetEnrichmentConfig
import org.sunbird.job.domain.`object`.DefinitionCache
import org.sunbird.job.exception.InvalidEventException
import org.sunbird.job.util.{CloudStorageUtil, JanusGraphUtil}
import org.sunbird.job.{BaseProcessFunction, Metrics}

import scala.collection.JavaConverters._

class ImageEnrichmentFunction(config: AssetEnrichmentConfig,
                              @transient var janusGraphUtil: JanusGraphUtil = null)
  extends BaseProcessFunction[Event, String](config)
    with ImageEnrichmentHelper with OptimizerHelper {

  private[this] val logger = LoggerFactory.getLogger(classOf[ImageEnrichmentFunction])
  lazy val definitionCache: DefinitionCache = new DefinitionCache
  lazy val cloudStorageUtil: CloudStorageUtil = new CloudStorageUtil(config)

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    janusGraphUtil = new JanusGraphUtil(config)
  }

  override def close(): Unit = {
    super.close()
  }

  @throws(classOf[InvalidEventException])
  override def processElement(event: Event, context: ProcessFunction[Event, String]#Context, metrics: Metrics): Unit = {
    logger.info(s"Received message for Image Enrichment for identifier : ${event.id}.")
    metrics.incCounter(config.imageEnrichmentEventCount)
    val asset = Asset(event.data)
    try {
      if (asset.validate(config.contentUploadContextDriven)) replaceArtifactUrl(asset)(cloudStorageUtil)
      asset.putAll(getMetadata(event.id)(janusGraphUtil))
      val mimeType = asset.get("mimeType", "").asInstanceOf[String]
      if (config.unsupportedMimeTypes.contains(mimeType) || !StringUtils.startsWithIgnoreCase(mimeType, "image")) {
        saveImageVariants(Map(), asset)(janusGraphUtil)
        metrics.incCounter(config.ignoredImageEnrichmentEventCount)
      } else {
        enrichImage(asset)(config, definitionCache, cloudStorageUtil, janusGraphUtil)
        metrics.incCounter(config.successImageEnrichmentEventCount)
      }
    } catch {
      case ex: Exception =>
        logger.error(s"Error while processing message for Image Enrichment for identifier : ${asset.identifier}.", ex)
        metrics.incCounter(config.failedImageEnrichmentEventCount)
        throw new InvalidEventException(ex.getMessage, Map("partition" -> event.partition, "offset" -> event.offset), ex)
    }
  }

  override def metricsList(): List[String] = {
    List(config.successImageEnrichmentEventCount, config.failedImageEnrichmentEventCount, config.imageEnrichmentEventCount, config.ignoredImageEnrichmentEventCount)
  }

  def getMetadata(identifier: String)(janusGraphUtil: JanusGraphUtil): Map[String, AnyRef] = {
    val metadata = janusGraphUtil.getNodeProperties(identifier)
    if (metadata != null && !metadata.isEmpty) metadata.asScala.toMap else throw new Exception(s"Received null or Empty metadata for identifier: $identifier.")
  }

}