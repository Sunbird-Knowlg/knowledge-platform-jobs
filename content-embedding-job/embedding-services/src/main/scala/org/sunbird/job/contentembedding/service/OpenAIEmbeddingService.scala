package org.sunbird.job.contentembedding.service

import org.slf4j.LoggerFactory
import org.sunbird.job.contentembedding.domain.EmbeddingServiceConfig
import org.sunbird.job.util.ScalaJsonUtil

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.time.Duration

/**
 * Embedding service backed by OpenAI or Azure OpenAI REST API.
 *
 * Supports both standard OpenAI and Azure OpenAI. Mode is selected automatically:
 *  - `azureEndpoint` non-empty → Azure mode: uses `api-key` header and Azure deployment URL.
 *  - `azureEndpoint` empty     → Standard OpenAI: uses `Authorization: Bearer` header.
 *
 * Standard OpenAI API:
 *  - Endpoint: `POST https://api.openai.com/v1/embeddings`
 *  - Request:  `{"model": "text-embedding-3-small", "input": ["text1", "text2"]}`
 *  - Response: `{"data": [{"index": 0, "embedding": [...]}, ...]}`
 *
 * Azure OpenAI API:
 *  - Endpoint: `POST <azureEndpoint>/openai/deployments/<deployment>/embeddings?api-version=<version>`
 *  - Same request/response format as standard OpenAI.
 *
 * @param config Requires `apiKey`. For standard OpenAI: `model`. For Azure: additionally
 *               `azureEndpoint`, `azureDeployment`, `azureApiVersion`.
 */
class OpenAIEmbeddingService(config: EmbeddingServiceConfig) extends EmbeddingService {

  private val logger     = LoggerFactory.getLogger(classOf[OpenAIEmbeddingService])
  private val httpClient = HttpClient.newBuilder()
    .connectTimeout(Duration.ofSeconds(config.timeoutSeconds))
    .build()

  private val apiKey = config.apiKey.getOrElse(
    throw new IllegalArgumentException("OpenAI api_key must be set in config: embedding.openai.api_key")
  )
  require(apiKey.nonEmpty, "OpenAI api_key is empty")

  // Azure mode when azureEndpoint is configured; falls back to standard OpenAI
  private val isAzure   = config.azureEndpoint.exists(_.nonEmpty)
  private val modelName = config.model.getOrElse("text-embedding-3-small")

  private val API_URL: String = if (isAzure) {
    val endpoint   = config.azureEndpoint.get.stripSuffix("/")
    val deployment = config.azureDeployment.getOrElse(modelName)
    val apiVersion = config.azureApiVersion.getOrElse("2024-12-01-preview")
    s"$endpoint/openai/deployments/$deployment/embeddings?api-version=$apiVersion"
  } else {
    "https://api.openai.com/v1/embeddings"
  }

  // Log only host, not full URL — Azure deployment name in query string can be sensitive.
  private val logSafeHost: String = try URI.create(API_URL).getHost catch { case _: Throwable => "unknown" }
  logger.info(s"OpenAIEmbeddingService ready: azure=$isAzure, host=$logSafeHost, dims=${config.dimensions}")

  override def getName: String = "openai"

  override def getVersion: String = "1.0"

  override def getDimensions: Int = config.dimensions

  override def embed(text: String): Array[Double] = embedBatch(List(text)).head

  // POST https://api.openai.com/v1/embeddings
  // Request:  {"model": "...", "input": ["text1", "text2"]}
  // Response: {"data": [{"index": 0, "embedding": [...]}, ...]}
  // Max 2048 inputs per call — caller controls batch size via config.
  override def embedBatch(texts: List[String]): List[Array[Double]] = {
    val requestBody = ScalaJsonUtil.serialize(Map("model" -> modelName, "input" -> texts))

    val authHeader = if (isAzure) ("api-key", apiKey) else ("Authorization", s"Bearer $apiKey")

    val request = HttpRequest.newBuilder()
      .uri(URI.create(API_URL))
      .header("Content-Type", "application/json")
      .header(authHeader._1, authHeader._2)
      .timeout(Duration.ofSeconds(config.timeoutSeconds))
      .POST(HttpRequest.BodyPublishers.ofString(requestBody))
      .build()

    val response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())

    if (response.statusCode() != 200) {
      // Surface status only; do not echo response body — it may quote our input or auth header.
      throw new RuntimeException(s"OpenAI API error ${response.statusCode()} (body suppressed)")
    }

    val responseMap = ScalaJsonUtil.deserialize[Map[String, Any]](response.body())
    val dataList    = responseMap("data").asInstanceOf[List[Map[String, Any]]]

    // Sort by index — OpenAI guarantees order but being explicit
    dataList.sortBy(_(  "index").asInstanceOf[Int])
      .map(_("embedding").asInstanceOf[List[Any]].map {
        case d: Double => d
        case f: Float  => f.toDouble
        case i: Int    => i.toDouble
        case n         => n.toString.toDouble
      }.toArray)
  }

  override def close(): Unit = logger.info("OpenAIEmbeddingService closed")
}
