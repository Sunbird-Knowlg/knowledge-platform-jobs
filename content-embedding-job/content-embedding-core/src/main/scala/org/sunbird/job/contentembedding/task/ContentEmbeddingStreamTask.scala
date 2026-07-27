package org.sunbird.job.contentembedding.task

import com.typesafe.config.ConfigFactory
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.slf4j.LoggerFactory
import org.sunbird.job.connector.FlinkKafkaConnector
import org.sunbird.job.contentembedding.domain.{ChunkedEvent, EmbeddedEvent, EmbeddingOutput, EnrichedMetadataEvent}
import org.sunbird.job.contentembedding.function.{BatchEmbeddingFunction, BatchedOpenSearchSinkFunction, ChunkingFunction, ExtractFunction, QuantizationFunction}
import org.sunbird.job.util.FlinkUtil

import java.io.File

/**
 * Wires the five-stage content embedding Flink pipeline.
 *
 * Pipeline stages (all connected via side outputs — main stream unused):
 * {{{
 *   Kafka input (enriched.content.metadata)
 *     └─► ExtractFunction           → enrichedOutTag
 *           └─► ChunkingFunction        → chunkedOutTag
 *                 └─► BatchEmbeddingFunction  → embeddedOutTag  (buffered, single API call per batch)
 *                       └─► QuantizationFunction    → quantizedOutTag
 *                             └─► BatchedOpenSearchSinkFunction → successOutTag / errorOutTag  (bulk update)
 * }}}
 *
 * Errors from every stage fan-in to a shared DLQ (Kafka error topic).
 * Successful object IDs are forwarded to the Kafka output topic.
 *
 * @param config         Job configuration.
 * @param kafkaConnector Kafka source/sink factory.
 */
class ContentEmbeddingStreamTask(config: ContentEmbeddingConfig, kafkaConnector: FlinkKafkaConnector) {

  private[this] val logger = LoggerFactory.getLogger(classOf[ContentEmbeddingStreamTask])

  private implicit val stringTypeInfo: TypeInformation[String] =
    TypeExtractor.getForClass(classOf[String])
  private implicit val intTypeInfo: TypeInformation[Int] =
    TypeExtractor.getForClass(classOf[Int])
  private implicit val enrichedEventTypeInfo: TypeInformation[EnrichedMetadataEvent] =
    TypeExtractor.getForClass(classOf[EnrichedMetadataEvent])
  private implicit val chunkedEventTypeInfo: TypeInformation[ChunkedEvent] =
    TypeExtractor.getForClass(classOf[ChunkedEvent])
  private implicit val embeddedEventTypeInfo: TypeInformation[EmbeddedEvent] =
    TypeExtractor.getForClass(classOf[EmbeddedEvent])
  private implicit val embeddingOutputTypeInfo: TypeInformation[EmbeddingOutput] =
    TypeExtractor.getForClass(classOf[EmbeddingOutput])

  def process(): Unit = {
    implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(config)
    buildGraph(env)
    env.execute(config.jobName)
  }

  def processForTest(env: StreamExecutionEnvironment): Unit = {
    buildGraph(env)
    env.execute(config.jobName)
  }

  private def buildGraph(env: StreamExecutionEnvironment): Unit = {
    logger.info("Building content embedding pipeline")

    val inputStream: DataStream[String] = env.fromSource(
      kafkaConnector.kafkaStringSource(config.kafkaInputTopic),
      WatermarkStrategy.noWatermarks(),
      config.inputConsumerName
    ).uid(config.inputConsumerName).setParallelism(config.kafkaConsumerParallelism)

    // Stage 1: Extract
    val extractStream = inputStream
      .process(new ExtractFunction(config))
      .name("extract-enriched-metadata").uid("extract-enriched-metadata")
      .setParallelism(config.extractParallelism)

    // Stage 2: Chunk — fed from side output of extract
    val chunkingStream = extractStream
      .getSideOutput(config.enrichedOutTag)
      .process(new ChunkingFunction(config))
      .name("chunk-content").uid("chunk-content")
      .setParallelism(config.chunkingParallelism)

    // Stage 3: Embed — keyed by bucket so each slot batches independently,
    // then flushed as a single API call per window/size threshold.
    // Local val avoids lambda capturing `this` (ContentEmbeddingStreamTask is not Serializable).
    val embeddingParallelism = config.embeddingParallelism
    val embeddingStream = chunkingStream
      .getSideOutput(config.chunkedOutTag)
      .keyBy(e => Math.abs(e.objectId.hashCode) % embeddingParallelism)
      .process(new BatchEmbeddingFunction(config))
      .name("batch-generate-embeddings").uid("batch-generate-embeddings")
      .setParallelism(config.embeddingParallelism)

    // Stage 4: Quantize — fed from side output of embedding
    val quantizationStream = embeddingStream
      .getSideOutput(config.embeddedOutTag)
      .process(new QuantizationFunction(config))
      .name("quantize-vectors").uid("quantize-vectors")
      .setParallelism(config.quantizationParallelism)

    // Stage 5: OpenSearch sink — keyed by constant 0 to access managed state + timers,
    // then bulk-flushed per size/interval threshold.
    val sinkStream = quantizationStream
      .getSideOutput(config.quantizedOutTag)
      .keyBy(_ => 0)
      .process(new BatchedOpenSearchSinkFunction(config))
      .name("batched-opensearch-sink").uid("batched-opensearch-sink")
      .setParallelism(config.sinkParallelism)

    // Success IDs → output topic
    sinkStream.getSideOutput(config.successOutTag)
      .sinkTo(kafkaConnector.kafkaStringSink(config.kafkaOutputTopic))

    // Error DLQ — collect from all stages
    val errorSink = kafkaConnector.kafkaStringSink(config.kafkaErrorTopic)
    extractStream.getSideOutput(config.errorOutTag).sinkTo(errorSink)
    chunkingStream.getSideOutput(config.errorOutTag).sinkTo(errorSink)
    embeddingStream.getSideOutput(config.errorOutTag).sinkTo(errorSink)
    quantizationStream.getSideOutput(config.errorOutTag).sinkTo(errorSink)
    sinkStream.getSideOutput(config.errorOutTag).sinkTo(errorSink)

    logger.info("Content embedding pipeline built")
  }
}

// $COVERAGE-OFF$
object ContentEmbeddingStreamTask {

  def main(args: Array[String]): Unit = {
    try {
      val configFilePath = Option(ParameterTool.fromArgs(args).get("config.file.path"))
      val config = configFilePath.map { path =>
        ConfigFactory.parseFile(new File(path)).resolve()
      }.getOrElse(ConfigFactory.load("content-embedding.conf").withFallback(ConfigFactory.systemEnvironment()))

      val embeddingConfig = new ContentEmbeddingConfig(config)
      val kafkaUtil = new FlinkKafkaConnector(embeddingConfig)
      val task = new ContentEmbeddingStreamTask(embeddingConfig, kafkaUtil)

      task.process()
    } catch {
      case e: Exception =>
        System.err.println(s"Job startup failed: ${e.getMessage}")
        e.printStackTrace(System.err)
        System.exit(1)
    }
  }
}
// $COVERAGE-ON$
