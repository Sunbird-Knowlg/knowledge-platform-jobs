package org.sunbird.job.contentembedding.function

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.contentembedding.domain.EmbeddingOutput
import org.sunbird.job.contentembedding.task.ContentEmbeddingConfig
import org.sunbird.job.util.{ElasticSearchUtil, ScalaJsonUtil}
import org.sunbird.job.{BaseProcessFunction, Metrics}

class OpenSearchSinkFunction(config: ContentEmbeddingConfig)(implicit stringTypeInfo: TypeInformation[String])
  extends BaseProcessFunction[EmbeddingOutput, String](config) {

  private[this] val logger = LoggerFactory.getLogger(classOf[OpenSearchSinkFunction])
  private var esUtil: ElasticSearchUtil = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    val connectionInfo = s"${config.openSearchHost}:${config.openSearchPort}"
    esUtil = new ElasticSearchUtil(connectionInfo, config.openSearchIndexName)
    logger.info(s"OpenSearch connected: $connectionInfo index=${config.openSearchIndexName}")
  }

  override def close(): Unit = {
    super.close()
    if (esUtil != null) esUtil.close()
  }

  override def metricsList(): List[String] = List(config.successEventCount, config.failedEventCount)

  override def processElement(
      output: EmbeddingOutput,
      context: ProcessFunction[EmbeddingOutput, String]#Context,
      metrics: Metrics
  ): Unit = {
    try {
      val docJson = buildChunksDocument(output)
      esUtil.updateDocument(output.objectId, docJson)

      logger.info(s"Updated OpenSearch chunks for ${output.objectId} (${output.contentType}, ${output.chunks.size} chunks)")
      metrics.incCounter(config.successEventCount)
      context.output(config.successOutTag, output.objectId)
    } catch {
      case e: Exception =>
        logger.error(s"OpenSearch update failed for ${output.objectId}: ${e.getMessage}", e)
        context.output(config.errorOutTag, s"""{"objectId":"${output.objectId}","stage":"opensearch","error":"${e.getMessage}"}""")
        metrics.incCounter(config.failedEventCount)
    }
  }

  private def buildChunksDocument(output: EmbeddingOutput): String = {
    val chunksData = output.chunks.map { chunk =>
      Map(
        "text"        -> chunk.text,
        "embedding"   -> chunk.embedding.map(_.toInt).toList,
        "token_count" -> chunk.tokenCount,
        "chunk_index" -> chunk.index
      )
    }
    ScalaJsonUtil.serialize(Map("chunks" -> chunksData))
  }
}
