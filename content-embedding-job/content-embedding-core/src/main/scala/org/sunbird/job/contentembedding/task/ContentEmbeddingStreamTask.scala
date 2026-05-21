package org.sunbird.job.contentembedding.task

import com.typesafe.config.ConfigFactory
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.slf4j.LoggerFactory
import org.sunbird.job.connector.FlinkKafkaConnector
import org.sunbird.job.contentembedding.domain.{ChunkedEvent, EmbeddedEvent, EmbeddingOutput, EnrichedMetadataEvent}
import org.sunbird.job.contentembedding.function.{ChunkingFunction, EmbeddingFunction, ExtractFunction, OpenSearchSinkFunction, QuantizationFunction}
import org.sunbird.job.util.{FlinkUtil, HttpUtil}

import java.io.File

class ContentEmbeddingStreamTask(config: ContentEmbeddingConfig, kafkaConnector: FlinkKafkaConnector, httpUtil: HttpUtil) {

  private[this] val logger = LoggerFactory.getLogger(classOf[ContentEmbeddingStreamTask])

  private implicit val stringTypeInfo: TypeInformation[String] =
    TypeExtractor.getForClass(classOf[String])
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

    // Stage 3: Embed — fed from side output of chunking
    val embeddingStream = chunkingStream
      .getSideOutput(config.chunkedOutTag)
      .process(new EmbeddingFunction(config))
      .name("generate-embeddings").uid("generate-embeddings")
      .setParallelism(config.embeddingParallelism)

    // Stage 4: Quantize — fed from side output of embedding
    val quantizationStream = embeddingStream
      .getSideOutput(config.embeddedOutTag)
      .process(new QuantizationFunction(config))
      .name("quantize-vectors").uid("quantize-vectors")
      .setParallelism(config.quantizationParallelism)

    // Stage 5: OpenSearch sink — fed from side output of quantization
    val sinkStream = quantizationStream
      .getSideOutput(config.quantizedOutTag)
      .process(new OpenSearchSinkFunction(config))
      .name("opensearch-sink").uid("opensearch-sink")
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
    val configFilePath = Option(ParameterTool.fromArgs(args).get("config.file.path"))
    val config = configFilePath.map { path =>
      ConfigFactory.parseFile(new File(path)).resolve()
    }.getOrElse(ConfigFactory.load("content-embedding.conf").withFallback(ConfigFactory.systemEnvironment()))

    val embeddingConfig = new ContentEmbeddingConfig(config)
    val kafkaUtil = new FlinkKafkaConnector(embeddingConfig)
    val httpUtil = new HttpUtil
    val task = new ContentEmbeddingStreamTask(embeddingConfig, kafkaUtil, httpUtil)
    task.process()
  }
}
// $COVERAGE-ON$
