package org.sunbird.job.knowlg.task

import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.scala.OutputTag
import org.sunbird.job.publish.config.PublishConfig
import org.sunbird.job.knowlg.publish.domain.Event

import java.util
import scala.collection.JavaConverters._

class KnowlgPublishConfig(override val config: Config) extends PublishConfig(config, "content-publish") {

  implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])
  implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])
  implicit val publishMetaTypeInfo: TypeInformation[Event] = TypeExtractor.getForClass(classOf[Event])

  // Job Configuration
  val jobEnv: String = config.getString("job.env")

  // Kafka Topics Configuration
  val kafkaInputTopic: String = config.getString("kafka.input.topic")
  val postPublishTopic: String = config.getString("kafka.post_publish.topic")
  val mvcTopic: String = config.getString("kafka.mvc.topic")
  val contentMetadataTopic: String = config.getString("kafka.content_metadata.topic")
  val kafkaErrorTopic: String = config.getString("kafka.error.topic")
  val inputConsumerName = "content-publish-consumer"

  // Parallelism
  override val kafkaConsumerParallelism: Int = config.getInt("task.consumer.parallelism")
  val eventRouterParallelism: Int = config.getInt("task.router.parallelism")

  // Metric List
  val totalEventsCount = "total-events-count"
  val skippedEventCount = "skipped-event-count"
  val contentPublishEventCount = "content-publish-count"
  val contentPublishSuccessEventCount = "content-publish-success-count"
  val contentPublishFailedEventCount = "content-publish-failed-count"
  val videoStreamingGeneratorEventCount = "video-streaming-event-count"
  val collectionPublishEventCount = "collection-publish-count"
  val collectionPublishSuccessEventCount = "collection-publish-success-count"
  val collectionPublishFailedEventCount = "collection-publish-failed-count"
  val collectionPostPublishProcessEventCount = "collection-post-publish-process-count"
  val mvProcessorEventCount = "mvc-processor-event-count"
  val dialcodeContextUpdaterEventCount = "dialcode-context-updater-event-count"

  // Cassandra Configurations
  val cassandraHost: String = config.getString("lms-cassandra.host")
  val cassandraPort: Int = config.getInt("lms-cassandra.port")
  val contentKeyspaceName: String = config.getString("content.keyspace")
  val contentTableName: String = config.getString("content.table")
  val hierarchyKeyspaceName: String = config.getString("hierarchy.keyspace")
  val hierarchyTableName: String = config.getString("hierarchy.table")

  // JanusGraph Configurations


  // Redis Configurations
  val nodeStore: Int = config.getInt("redis.database.contentCache.id")

  // Question/QuestionSet Configurations (merged from questionset-publish)
  val questionKeyspaceName: String = if (config.hasPath("question.keyspace")) config.getString("question.keyspace") else contentKeyspaceName
  val questionTableName: String = if (config.hasPath("question.table")) config.getString("question.table") else contentTableName
  val questionSetKeyspaceName: String = if (config.hasPath("questionset.keyspace")) config.getString("questionset.keyspace") else hierarchyKeyspaceName
  val questionSetTableName: String = if (config.hasPath("questionset.table")) config.getString("questionset.table") else hierarchyTableName
  val cacheDbId: Int = if (config.hasPath("redis.database.qsCache.id")) config.getInt("redis.database.qsCache.id") else nodeStore

  // Question/QuestionSet Metrics
  val questionPublishEventCount = "question-publish-count"
  val questionPublishSuccessEventCount = "question-publish-success-count"
  val questionPublishFailedEventCount = "question-publish-failed-count"
  val questionSetPublishEventCount = "questionset-publish-count"
  val questionSetPublishSuccessEventCount = "questionset-publish-success-count"
  val questionSetPublishFailedEventCount = "questionset-publish-failed-count"

  // Out Tags
  val contentPublishOutTag: OutputTag[Event] = OutputTag[Event]("content-publish")
  val collectionPublishOutTag: OutputTag[Event] = OutputTag[Event]("collection-publish")
  val questionPublishOutTag: OutputTag[Event] = OutputTag[Event]("question-publish")
  val questionSetPublishOutTag: OutputTag[Event] = OutputTag[Event]("questionset-publish")
  val generateVideoStreamingOutTag: OutputTag[String] = OutputTag[String]("video-streaming-generator-request")
  val failedEventOutTag: OutputTag[String] = OutputTag[String]("failed-event")
  val generatePostPublishProcessTag: OutputTag[String] = OutputTag[String]("post-publish-process-request")
  val mvcProcessorTag: OutputTag[String] = OutputTag[String]("mvc-processor-request")
  val dialcodeContextUpdaterOutTag: OutputTag[String] = OutputTag[String]("dialcode-context-updater-request")
  val contentMetadataEventOutTag: OutputTag[String] = OutputTag[String]("content-metadata-event-request")


  val definitionBasePath: String = if (config.hasPath("schema.basePath")) config.getString("schema.basePath") else "https://sunbirddev.blob.core.windows.net/sunbird-content-dev/schemas/local"
  val schemaSupportVersionMap: Map[String, AnyRef] = if (config.hasPath("schema.supportedVersion")) config.getObject("schema.supportedVersion").unwrapped().asScala.toMap else Map[String, AnyRef]()

  val supportedObjectType: util.List[String] = if (config.hasPath("content.objectType")) config.getStringList("content.objectType") else util.Arrays.asList[String]("Content", "ContentImage", "Collection", "CollectionImage", "Question", "QuestionImage", "QuestionSet", "QuestionSetImage")
  val supportedMimeType: util.List[String] = if (config.hasPath("content.mimeType")) config.getStringList("content.mimeType") else util.Arrays.asList[String]("application/pdf", "application/vnd.sunbird.question", "application/vnd.sunbird.questionset")
  val streamableMimeType: util.List[String] = if (config.hasPath("content.stream.mimeType")) config.getStringList("content.stream.mimeType") else util.Arrays.asList[String]("video/mp4")
  val isStreamingEnabled: Boolean = if (config.hasPath("content.stream.enabled")) config.getBoolean("content.stream.enabled") else false
  val assetDownloadDuration: String = if (config.hasPath("content.asset_download_duration")) config.getString("content.asset_download_duration") else "60 seconds"

  val isECARExtractionEnabled: Boolean = if (config.hasPath("content.isECARExtractionEnabled")) config.getBoolean("content.isECARExtractionEnabled") else true
  val contentFolder: String = if (config.hasPath("cloud_storage.folder.content")) config.getString("cloud_storage.folder.content") else "content"
  val artifactFolder: String = if (config.hasPath("cloud_storage.folder.artifact")) config.getString("cloud_storage.folder.artifact") else "artifact"
  val retryAssetDownloadsCount: Integer = if (config.hasPath("content.retry_asset_download_count")) config.getInt("content.retry_asset_download_count") else 1
  val artifactSizeForOnline: Double = if (config.hasPath("content.artifact.size.for_online")) config.getDouble("content.artifact.size.for_online") else 209715200
  val bundleLocation: String = if (config.hasPath("content.bundleLocation")) config.getString("content.bundleLocation") else "/data/contentBundle/"

  val extractableMimeTypes = List("application/vnd.ekstep.ecml-archive", "application/vnd.ekstep.html-archive", "application/vnd.ekstep.plugin-archive", "application/vnd.ekstep.h5p-archive")

  val categoryMap: java.util.Map[String, AnyRef] = if (config.hasPath("contentTypeToPrimaryCategory")) config.getAnyRef("contentTypeToPrimaryCategory").asInstanceOf[java.util.Map[String, AnyRef]] else new util.HashMap[String, AnyRef]()

  val esConnectionInfo: String = config.getString("es.basePath")
  val compositeSearchIndexName: String = if (config.hasPath("compositesearch.index.name")) config.getString("compositesearch.index.name") else "compositesearch"
  val compositeSearchIndexType: String = if (config.hasPath("search.document.type")) config.getString("search.document.type") else "cs"
  val nestedFields: util.List[String] = if (config.hasPath("content.nested.fields")) config.getStringList("content.nested.fields") else util.Arrays.asList[String]("badgeAssertions","targets","badgeAssociations")

  val allowedExtensionsWord: util.List[String] = if (config.hasPath("mimetype.allowed_extensions.word")) config.getStringList("mimetype.allowed_extensions.word") else util.Arrays.asList[String]("doc", "docx", "ppt", "pptx", "key", "odp", "pps", "odt", "wpd", "wps", "wks")
  val enableDIALContextUpdate: String = if (config.hasPath("enableDIALContextUpdate")) config.getString("enableDIALContextUpdate") else "No"

  val isrRelativePathEnabled: Boolean = if (config.hasPath("cloudstorage.metadata.replace_absolute_path")) config.getBoolean("cloudstorage.metadata.replace_absolute_path") else false

  val isAISearchEnabled: Boolean = if (config.hasPath("ai_search_enabled")) config.getBoolean("ai_search_enabled") else false
}
