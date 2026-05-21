package org.sunbird.job.contentembedding.service

import org.slf4j.LoggerFactory
import org.sunbird.job.contentembedding.domain.EmbeddingServiceConfig
import org.sunbird.job.util.ScalaJsonUtil

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.time.Duration

class OpenAIEmbeddingService(config: EmbeddingServiceConfig) extends EmbeddingService {

  private val logger     = LoggerFactory.getLogger(classOf[OpenAIEmbeddingService])
  private val API_URL    = "https://api.openai.com/v1/embeddings"
  private val httpClient = HttpClient.newBuilder()
    .connectTimeout(Duration.ofSeconds(config.timeoutSeconds))
    .build()

  private val apiKey = config.apiKey.getOrElse(
    throw new IllegalArgumentException("OpenAI api_key must be set in config: embedding.openai.api_key")
  )
  private val modelName = config.model.getOrElse("text-embedding-3-small")

  require(apiKey.nonEmpty, "OpenAI api_key is empty")

  logger.info(s"OpenAIEmbeddingService ready: model=${config.model}, dims=${config.dimensions}")

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

    val request = HttpRequest.newBuilder()
      .uri(URI.create(API_URL))
      .header("Content-Type", "application/json")
      .header("Authorization", s"Bearer $apiKey")
      .timeout(Duration.ofSeconds(config.timeoutSeconds))
      .POST(HttpRequest.BodyPublishers.ofString(requestBody))
      .build()

    val response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())

    if (response.statusCode() != 200)
      throw new RuntimeException(s"OpenAI API error ${response.statusCode()}: ${response.body().take(300)}")

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
