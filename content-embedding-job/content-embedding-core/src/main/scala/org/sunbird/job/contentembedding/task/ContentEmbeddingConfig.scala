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
  val excludedFields: Set[String] = if (config.hasPath("chunking.semantic.excluded_fields")) {
    config.getStringList("chunking.semantic.excluded_fields").stream.collect(java.util.stream.Collectors.toSet()).asScala.toSet
  } else {
    Set("hierarchy", "children", "id", "identifier", "contentType", "_schema_version", "timestamp")
  }
  // sliding-window strategy config
  val maxTokens: Int              = getInt("chunking.sliding-window.max_tokens", 512)
  val overlapTokens: Int          = getInt("chunking.sliding-window.overlap_tokens", 102)

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
      serviceName     = "openai",
      dimensions      = getInt("embedding.openai.dimensions", 1536),
      timeoutSeconds  = getInt("embedding.openai.timeout", 30),
      apiKey          = Some(getString("embedding.openai.api_key", "")),
      model           = Some(getString("embedding.openai.model", "text-embedding-3-small")),
      azureEndpoint   = Some(getString("embedding.openai.azure_endpoint", "")),
      azureApiVersion = Some(getString("embedding.openai.azure_api_version", "2024-12-01-preview")),
      azureDeployment = Some(getString("embedding.openai.azure_deployment", "text-embedding-3-small"))
    )
    case name => EmbeddingServiceConfig(serviceName = name)
  }

  def quantizationStrategyConfig: QuantizationConfig = QuantizationConfig(strategyName = quantizationStrategy)

  def chunkingStrategyConfig: ChunkingConfig = ChunkingConfig(
    strategyName   = chunkingStrategy,
    maxChunkSize   = maxChunkSize,
    maxTokens      = maxTokens,
    overlapTokens  = overlapTokens,
    excludedFields = excludedFields
  )
}
