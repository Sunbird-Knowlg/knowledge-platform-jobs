package org.sunbird.job.assetenricment.task

import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.scala.OutputTag
import org.sunbird.job.BaseJobConfig
import org.sunbird.job.assetenricment.domain.Event

import java.util
import scala.collection.JavaConverters._

class AssetEnrichmentConfig(override val config: Config) extends BaseJobConfig(config, "asset-enrichment") {

  implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])
  implicit val eventTypeInfo: TypeInformation[Event] = TypeExtractor.getForClass(classOf[Event])

  // Job Configuration
  val jobEnv: String = config.getString("job.env")

  // Kafka Topics Configuration
  val kafkaInputTopic: String = config.getString("kafka.input.topic")
  val videoStreamingTopic: String = config.getString("kafka.video_stream.topic")

  // Parallelism
  val eventRouterParallelism: Int = config.getInt("task.router.parallelism")
  val videoEnrichmentIndexerParallelism: Int = config.getInt("task.videoEnrichment.parallelism")
  val imageEnrichmentIndexerParallelism: Int = config.getInt("task.imageEnrichment.parallelism")

  // Consumers
  val assetEnrichmentConsumer = "asset-enrichment-consumer"
  val assetEnrichmentRouter = "asset-enrichment-router"

  // Metric List
  val totalEventsCount = "total-events-count"
  val skippedEventCount = "skipped-event-count"
  val imageEnrichmentEventCount = "image-enrichment-event-count"
  val successImageEnrichmentEventCount = "success-image-enrichment-event-count"
  val ignoredImageEnrichmentEventCount = "skipped-image-enrichment-event-count"
  val failedImageEnrichmentEventCount = "failed-image-enrichment-event-count"
  val successVideoEnrichmentEventCount = "success-video-enrichment-event-count"
  val failedVideoEnrichmentEventCount = "failed-video-enrichment-event-count"
  val videoEnrichmentEventCount = "video-enrichment-event-count"
  val videoStreamingGeneratorEventCount = "video-streaming-event-count"

  // Neo4J Configurations
  val graphRoutePath: String = config.getString("neo4j.routePath")
  val graphName: String = config.getString("neo4j.graph")

  // Tags
  val imageEnrichmentDataOutTag: OutputTag[Event] = OutputTag[Event]("image-enrichment-data")
  val videoEnrichmentDataOutTag: OutputTag[Event] = OutputTag[Event]("video-enrichment-data")
  val generateVideoStreamingOutTag: OutputTag[String] = OutputTag[String]("video-streaming-generator-request")

  // Asset Variables
  val contentUploadContextDriven: Boolean = if (config.hasPath("content.upload.context.driven")) config.getBoolean("content.upload.context.driven") else true
  val maxIterationCount: Int = if (config.hasPath("content.max.iteration.count")) config.getInt("content.max.iteration.count") else 2

  // Video Enrichment
  val youtubeAppName: String = if (config.hasPath("content.youtube.applicationName")) config.getString("content.youtube.applicationName") else "fetch-youtube-license"
  val videoIdRegex: util.List[String] = if (config.hasPath("content.youtube.regexPattern")) config.getStringList("content.youtube.regexPattern") else util.Arrays.asList[String]("\\?vi?=([^&]*)", "watch\\?.*v=([^&]*)", "(?:embed|vi?)/([^/?]*)", "^([A-Za-z0-9\\-\\_]*)")
  val streamableMimeType: util.List[String] = if (config.hasPath("content.stream.mimeType")) config.getStringList("content.stream.mimeType") else util.Arrays.asList[String]("video/mp4")
  val isStreamingEnabled: Boolean = if (config.hasPath("content.stream.enabled")) config.getBoolean("content.stream.enabled") else false
  val sampleThumbnailCount: Int = if(config.hasPath("thumbnail.max.sample")) config.getInt("thumbnail.max.sample") else 5
  val thumbnailSize: Int = if(config.hasPath("thumbnail.max.size.pixel")) config.getInt("thumbnail.max.size.pixel") else 150

  // Schema Definition Util for Image Enrichment
  val definitionBasePath: String = if (config.hasPath("schema.base_path")) config.getString("schema.base_path") else "https://sunbirddev.blob.core.windows.net/sunbird-content-dev/schemas/local"
  val schemaSupportVersionMap: Map[String, String] = if (config.hasPath("schema.supported_version")) config.getAnyRef("schema.supported_version").asInstanceOf[util.Map[String, String]].asScala.toMap else Map[String, String]()

  val unsupportedMimeTypes: util.List[String] = if (config.hasPath("unsupported.mimetypes")) config.getStringList("unsupported.mimetypes") else util.Arrays.asList[String]("image/svg+xml")
  def getString(key: String, default: String): String = {
    if (config.hasPath(key)) config.getString(key) else default
  }
}
