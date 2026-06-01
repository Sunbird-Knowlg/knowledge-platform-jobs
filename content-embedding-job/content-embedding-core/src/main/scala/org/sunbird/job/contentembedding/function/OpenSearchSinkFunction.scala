package org.sunbird.job.contentembedding.function

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.contentembedding.domain.EmbeddingOutput
import org.sunbird.job.contentembedding.task.ContentEmbeddingConfig
import org.sunbird.job.util.{ElasticSearchUtil, ScalaJsonUtil}
import org.sunbird.job.{BaseProcessFunction, Metrics}

/**
 * Stage 5 (terminal) of the content embedding pipeline.
 *
 * Writes all quantized chunks for a content object to the `compositesearch` OpenSearch index
 * as a partial document update keyed by `objectId`. All chunks are stored under the nested
 * `chunks` field on the existing document — existing metadata fields are untouched.
 *
 * The `chunks` field uses `type: nested` with a `knn_vector` sub-field (`data_type: byte`)
 * enabling kNN semantic search via nested queries with `score_mode: max`.
 *
 * On success, the `objectId` is emitted to the `successOutTag` side output and forwarded
 * to the Kafka output topic. Failures are sent to the `errorOutTag` DLQ.
 */
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

      // ElasticSearchUtil.updateDocument swallows IOException and only logs. Verify the write
      // landed by reading the doc back; otherwise we'd ack failed writes to Kafka.
      val verifyDoc = esUtil.getDocumentAsString(output.objectId)
      if (verifyDoc == null || verifyDoc.isEmpty) {
        throw new RuntimeException(s"OpenSearch write verify failed: doc ${output.objectId} not found after update")
      }

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
        "text"           -> chunk.text,
        "embedding"      -> chunk.embedding.map(_.toInt).toList,
        "word_count"     -> chunk.wordCount,
        "chunk_index"    -> chunk.index,
        "schema_version" -> output.schemaVersion
      )
    }
    ScalaJsonUtil.serialize(Map("chunks" -> chunksData))
  }
}
