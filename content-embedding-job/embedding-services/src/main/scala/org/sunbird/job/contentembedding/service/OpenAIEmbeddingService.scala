package org.sunbird.job.contentembedding.service

import org.slf4j.LoggerFactory
import org.sunbird.job.contentembedding.domain.EmbeddingServiceConfig
import org.sunbird.job.util.ScalaJsonUtil

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.time.Duration
import scala.util.Random

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
  // Transient errors (429, 5xx) are retried with exponential backoff + jitter.
  // Non-transient 4xx (401, 403, 400) fail immediately without retry.
  override def embedBatch(texts: List[String]): List[Array[Double]] = {
    val requestBody = ScalaJsonUtil.serialize(Map("model" -> modelName, "input" -> texts))
    val authHeader  = if (isAzure) ("api-key", apiKey) else ("Authorization", s"Bearer $apiKey")

    def attempt(retriesLeft: Int): List[Array[Double]] = {
      val request = HttpRequest.newBuilder()
        .uri(URI.create(API_URL))
        .header("Content-Type", "application/json")
        .header(authHeader._1, authHeader._2)
        .timeout(Duration.ofSeconds(config.timeoutSeconds))
        .POST(HttpRequest.BodyPublishers.ofString(requestBody))
        .build()

      val response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())
      val status   = response.statusCode()

      if (status == 200) {
        val responseMap = ScalaJsonUtil.deserialize[Map[String, Any]](response.body())
        val dataList    = responseMap("data").asInstanceOf[List[Map[String, Any]]]
        dataList.sortBy(_("index").asInstanceOf[Int])
          .map(_("embedding").asInstanceOf[List[Any]].map {
            case d: Double => d
            case f: Float  => f.toDouble
            case i: Int    => i.toDouble
            case n         => n.toString.toDouble
          }.toArray)
      } else if ((status == 429 || status >= 500) && retriesLeft > 0) {
        val attempt   = config.maxRetries - retriesLeft + 1
        val jitter    = (Random.nextDouble() * config.retryBaseDelayMs).toLong
        val delayMs   = (config.retryBaseDelayMs * Math.pow(2, attempt - 1).toLong) + jitter
        logger.warn(s"OpenAI API transient error $status, retry $attempt/${config.maxRetries} in ${delayMs}ms")
        Thread.sleep(delayMs)
        this.attempt(retriesLeft - 1)
      } else {
        // Surface status only; do not echo response body — it may quote our input or auth header.
        throw new RuntimeException(s"OpenAI API error $status (body suppressed)")
      }
    }

    attempt(config.maxRetries)
  }

  override def close(): Unit = logger.info("OpenAIEmbeddingService closed")
}
