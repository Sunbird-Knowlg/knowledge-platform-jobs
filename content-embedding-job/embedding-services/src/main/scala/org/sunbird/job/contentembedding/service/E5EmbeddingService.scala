package org.sunbird.job.contentembedding.service

import org.slf4j.LoggerFactory
import org.sunbird.job.contentembedding.domain.EmbeddingServiceConfig
import org.sunbird.job.util.ScalaJsonUtil

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.time.Duration

/**
 * Embedding service backed by a HuggingFace Text Embeddings Inference (TEI) server
 * running `intfloat/multilingual-e5-large`.
 *
 * TEI API: `POST /embed`
 *  - Request:  `{"inputs": ["passage: text1", "passage: text2", ...]}`
 *  - Response: `[[v1, v2, ...], [...]]`  (raw array of arrays, no wrapper object)
 *
 * The `"passage: "` prefix is required by E5 instruction-tuned models for content
 * being indexed. Use `"query: "` prefix at search time (not applied here).
 *
 * @param config Expects `host` and `port` fields pointing to the TEI server.
 *               Defaults: host=localhost, port=80.
 */
class E5EmbeddingService(config: EmbeddingServiceConfig) extends EmbeddingService {

  private[this] val logger  = LoggerFactory.getLogger(classOf[E5EmbeddingService])
  private val rawHost       = config.host.getOrElse("localhost")
  // Restrict TEI host to internal/private targets so a misconfigured env var can't
  // redirect embedding traffic (which contains user content) to an external host.
  require(isInternalHost(rawHost),
    s"E5 host '$rawHost' is not an internal/private hostname; refusing to send content to public targets")
  private val endpointUrl   = s"http://$rawHost:${config.port.getOrElse(80)}/embed"
  private val httpClient    = HttpClient.newBuilder()
    .connectTimeout(Duration.ofSeconds(config.timeoutSeconds))
    .build()

  private def isInternalHost(host: String): Boolean = {
    if (host == null || host.isEmpty) return false
    val lower = host.toLowerCase
    lower == "localhost" ||
      lower.startsWith("127.") ||
      lower.startsWith("10.") ||
      lower.startsWith("192.168.") ||
      (lower.startsWith("172.") && {
        val parts = lower.split('.')
        parts.length >= 2 && {
          val second = try parts(1).toInt catch { case _: Throwable => -1 }
          second >= 16 && second <= 31
        }
      }) ||
      lower.endsWith(".svc.cluster.local") ||
      lower.endsWith(".cluster.local") ||
      lower.endsWith(".internal") ||
      !lower.contains('.') // bare k8s service name
  }

  logger.info(s"E5EmbeddingService ready: $endpointUrl (${config.dimensions}d)")

  override def getName: String = "e5"

  override def getVersion: String = "2.0"

  override def getDimensions: Int = config.dimensions

  override def embed(text: String): Array[Double] = embedBatch(List(text)).head

  // HuggingFace Text Embeddings Inference (TEI) API:
  // POST /embed
  // Request:  {"inputs": ["passage: text1", "passage: text2"]}
  // Response: [[0.023, -0.11, ...], [...]]   ← array of arrays, no wrapper object
  //
  // "passage: " prefix required by E5 models for content being indexed.
  // "query: " prefix is used at search time (not here).
  override def embedBatch(texts: List[String]): List[Array[Double]] = {
    val prefixed    = texts.map(t => s"passage: $t")
    val requestBody = ScalaJsonUtil.serialize(Map("inputs" -> prefixed))

    val request = HttpRequest.newBuilder()
      .uri(URI.create(endpointUrl))
      .header("Content-Type", "application/json")
      .timeout(Duration.ofSeconds(config.timeoutSeconds))
      .POST(HttpRequest.BodyPublishers.ofString(requestBody))
      .build()

    val response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())

    if (response.statusCode() != 200)
      throw new RuntimeException(s"TEI server error ${response.statusCode()}: ${response.body().take(200)}")

    // TEI returns a raw array of arrays: [[v1, v2, ...], [v1, v2, ...]]
    val embeddingsList = ScalaJsonUtil.deserialize[List[List[Any]]](response.body())

    embeddingsList.map(row => row.map {
      case d: Double => d
      case f: Float  => f.toDouble
      case i: Int    => i.toDouble
      case n         => n.toString.toDouble
    }.toArray)
  }

  override def close(): Unit = logger.info("E5EmbeddingService closed")
}
