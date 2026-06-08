package org.sunbird.job.contentembedding.task

import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.scala.OutputTag
import org.sunbird.job.BaseJobConfig
import org.sunbird.job.contentembedding.domain.{ChunkedEvent, ChunkingConfig, EmbeddedEvent, EmbeddingOutput, EmbeddingServiceConfig, EnrichedMetadataEvent, QuantizationConfig}
import scala.collection.JavaConverters._

class ContentEmbeddingConfig(override val config: Config) extends BaseJobConfig(config, "content-embedding") {

  implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

  // Job Configuration
  val jobEnv: String = config.getString("job.env")

  // Kafka Topics
  val kafkaInputTopic: String = config.getString("kafka.input.topic")
  val kafkaOutputTopic: String = config.getString("kafka.output.topic")
  val kafkaErrorTopic: String = config.getString("kafka.error.topic")
  val inputConsumerName = "content-embedding-consumer"

  // Parallelism
  override val kafkaConsumerParallelism: Int = config.getInt("task.consumer.parallelism")
  val extractParallelism: Int = config.getInt("task.extract.parallelism")
  val chunkingParallelism: Int = config.getInt("task.chunking.parallelism")
  val embeddingParallelism: Int = config.getInt("task.embedding.parallelism")
  val quantizationParallelism: Int = config.getInt("task.quantization.parallelism")
  val sinkParallelism: Int = config.getInt("task.sink.parallelism")

  // Embedding Configuration
  val chunkingStrategy: String    = getString("chunking.strategy", "semantic")
  val embeddingService: String    = getString("embedding.service", "e5")
  val quantizationStrategy: String = getString("quantization.strategy", "int8")
  val embeddingBatchSize: Int     = getInt("embedding.batch_size", 32)
  // semantic strategy config
  val maxChunkSize: Int           = getInt("chunking.semantic.max_chunk_size", 1000)
  // Fields excluded from metadata extraction — applies to both chunking strategies.
  // Falls back to ChunkingConfig defaults when not set in config file.
  val excludedFields: Set[String] = if (config.hasPath("chunking.semantic.excluded_fields")) {
    config.getStringList("chunking.semantic.excluded_fields").asScala.toSet
  } else ChunkingConfig("semantic").excludedFields
  // sliding-window strategy config
  val maxWords: Int               = getInt("chunking.sliding-window.max_words", 512)
  val overlapWords: Int           = getInt("chunking.sliding-window.overlap_words", 102)

  // Supported schema versions for incoming enriched events. Events outside
  // this list are routed to the DLQ rather than silently processed.
  val supportedSchemaVersions: Set[String] =
    if (config.hasPath("schema.supported_versions"))
      config.getStringList("schema.supported_versions").asScala.toSet
    else Set("1.0")

  // OpenSearch Configuration
  val openSearchHost: String = config.getString("opensearch.host")
  val openSearchPort: Int = config.getInt("opensearch.port")
  val openSearchIndexName: String = if (config.hasPath("opensearch.index.name")) config.getString("opensearch.index.name") else "compositesearch"

  // Metrics
  val totalEventsCount = "total-events-count"
  val filteredEventsCount = "filtered-events-count"
  val extractedEventsCount = "extracted-events-count"
  val chunkedEventsCount = "chunked-events-count"
  val embeddedEventsCount = "embedded-events-count"
  val quantizedEventsCount = "quantized-events-count"
  val successEventCount = "success-event-count"
  val failedEventCount = "failed-event-count"
  val embeddingSlowCallCount = "embedding-slow-call-count"
  val embeddingApiCallCount  = "embedding-api-call-count"

  // Log a warning when embedBatch exceeds this many millis. Default 5s.
  val embeddingSlowCallThresholdMs: Long =
    if (config.hasPath("embedding.slow_call_threshold_ms"))
      config.getLong("embedding.slow_call_threshold_ms")
    else 5000L

  // TypeInformation needed by OutputTag constructors
  implicit val enrichedMetadataEventTypeInfo: TypeInformation[EnrichedMetadataEvent] =
    TypeExtractor.getForClass(classOf[EnrichedMetadataEvent])
  implicit val chunkedEventTypeInfo: TypeInformation[ChunkedEvent] =
    TypeExtractor.getForClass(classOf[ChunkedEvent])
  implicit val embeddedEventTypeInfo: TypeInformation[EmbeddedEvent] =
    TypeExtractor.getForClass(classOf[EmbeddedEvent])
  implicit val embeddingOutputTypeInfo: TypeInformation[EmbeddingOutput] =
    TypeExtractor.getForClass(classOf[EmbeddingOutput])

  // Output Tags — pipeline routing via side outputs (Flink codebase pattern)
  val errorOutTag: OutputTag[String]                     = OutputTag[String]("embedding-error")
  val successOutTag: OutputTag[String]                   = OutputTag[String]("embedding-success")
  val enrichedOutTag: OutputTag[EnrichedMetadataEvent]   = OutputTag[EnrichedMetadataEvent]("enriched-metadata")
  val chunkedOutTag: OutputTag[ChunkedEvent]             = OutputTag[ChunkedEvent]("chunked-event")
  val embeddedOutTag: OutputTag[EmbeddedEvent]           = OutputTag[EmbeddedEvent]("embedded-event")
  val quantizedOutTag: OutputTag[EmbeddingOutput]        = OutputTag[EmbeddingOutput]("quantized-event")

  def embeddingServiceConfig: EmbeddingServiceConfig = embeddingService match {
    case "e5" => EmbeddingServiceConfig(
      serviceName    = "e5",
      dimensions     = getInt("embedding.e5.dimensions", 768),
      timeoutSeconds = getInt("embedding.timeout", 30),
      host           = Some(getString("embedding.e5.host", "localhost")),
      port           = Some(getInt("embedding.e5.port", 8000))
    )
    case "openai" => EmbeddingServiceConfig(
      serviceName      = "openai",
      dimensions       = getInt("embedding.openai.dimensions", 1536),
      timeoutSeconds   = getInt("embedding.openai.timeout", 30),
      apiKey           = Some(getString("embedding.openai.api_key", "")),
      model            = Some(getString("embedding.openai.model", "text-embedding-3-small")),
      azureEndpoint    = Some(getString("embedding.openai.azure_endpoint", "")),
      azureApiVersion  = Some(getString("embedding.openai.azure_api_version", "2024-12-01-preview")),
      azureDeployment  = Some(getString("embedding.openai.azure_deployment", "text-embedding-3-small")),
      maxRetries       = getInt("embedding.openai.max_retries", 3),
      retryBaseDelayMs = getInt("embedding.openai.retry_base_delay_ms", 500).toLong
    )
    case name => EmbeddingServiceConfig(serviceName = name)
  }

  // Fail fast on misconfiguration rather than at first embed call.
  if (embeddingService == "openai") {
    val apiKey = getString("embedding.openai.api_key", "")
    require(apiKey.nonEmpty,
      "embedding.openai.api_key must be set (env OPENAI_API_KEY) when embedding.service=openai")
  }

  def quantizationStrategyConfig: QuantizationConfig = QuantizationConfig(strategyName = quantizationStrategy)

  def chunkingStrategyConfig: ChunkingConfig = ChunkingConfig(
    strategyName   = chunkingStrategy,
    maxChunkSize   = maxChunkSize,
    maxWords       = maxWords,
    overlapWords   = overlapWords,
    excludedFields = excludedFields
  )
}
