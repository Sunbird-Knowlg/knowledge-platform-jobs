package org.sunbird.job.contentembedding.function

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.contentembedding.domain.{ChunkEmbedding, EmbeddedEvent, EmbeddingOutput, VectorEmbedding}
import org.sunbird.job.contentembedding.factory.QuantizationStrategyFactory
import org.sunbird.job.contentembedding.task.ContentEmbeddingConfig
import org.sunbird.job.util.ScalaJsonUtil
import org.sunbird.job.{BaseProcessFunction, Metrics}

/**
 * Stage 4 of the content embedding pipeline.
 *
 * Compresses float32 embedding vectors to int8 using the configured
 * [[org.sunbird.job.contentembedding.service.QuantizationStrategy]].
 *
 * For L2-normalised vectors (all OpenAI / E5 outputs) the global-scale path is used:
 * `byte = round(v × 127)`, achieving 4× storage reduction with &lt;2% recall loss.
 *
 * Quantized events are emitted via the `quantizedOutTag` side output as [[EmbeddingOutput]],
 * ready for the OpenSearch sink.
 */
class QuantizationFunction(config: ContentEmbeddingConfig)(implicit stringTypeInfo: TypeInformation[String])
  extends BaseProcessFunction[EmbeddedEvent, String](config) {

  private[this] val logger = LoggerFactory.getLogger(classOf[QuantizationFunction])
  private var quantizationStrategy: org.sunbird.job.contentembedding.service.QuantizationStrategy = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    quantizationStrategy = QuantizationStrategyFactory.getStrategy(config.quantizationStrategyConfig)
    logger.info(s"Initialized QuantizationStrategy: ${quantizationStrategy.getName} v${quantizationStrategy.getVersion}")
  }

  override def metricsList(): List[String] = List(config.quantizedEventsCount, config.failedEventCount)

  override def processElement(
      event: EmbeddedEvent,
      context: ProcessFunction[EmbeddedEvent, String]#Context,
      metrics: Metrics
  ): Unit = {
    try {
      val chunkEmbeddings = event.chunks.map { embeddedChunk =>
        val vectorEmbedding = VectorEmbedding(
          chunkIndex = embeddedChunk.chunkIndex,
          vector = embeddedChunk.vector,
          modelId = embeddedChunk.modelId,
          dimensions = embeddedChunk.vector.length,
          wordCount = embeddedChunk.wordCount
        )
        val quantized = quantizationStrategy.quantize(vectorEmbedding)

        ChunkEmbedding(
          text = embeddedChunk.text,
          sourceField = embeddedChunk.sourceField,
          embedding = quantized.vector,
          index = embeddedChunk.chunkIndex,
          wordCount = embeddedChunk.wordCount
        )
      }

      logger.info(s"Quantized ${chunkEmbeddings.size} chunks for ${event.objectId}")
      metrics.incCounter(config.quantizedEventsCount)

      context.output(config.quantizedOutTag, EmbeddingOutput(
        objectId = event.objectId,
        contentType = event.contentType,
        chunks = chunkEmbeddings,
        embeddingModel = event.chunks.headOption.map(_.modelId).getOrElse(config.embeddingService),
        quantizationType = quantizationStrategy.getName,
        timestamp = System.currentTimeMillis(),
        schemaVersion = event.schemaVersion
      ))
    } catch {
      case e: Exception =>
        logger.error(s"Error quantizing ${event.objectId}: ${e.getMessage}", e)
        context.output(config.errorOutTag, ScalaJsonUtil.serialize(Map("objectId" -> event.objectId, "stage" -> "quantization", "error" -> e.getMessage)))
        metrics.incCounter(config.failedEventCount)
    }
  }
}
