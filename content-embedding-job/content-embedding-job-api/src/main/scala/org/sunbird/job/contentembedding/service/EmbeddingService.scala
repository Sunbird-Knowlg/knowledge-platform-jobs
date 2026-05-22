package org.sunbird.job.contentembedding.service

/**
 * Contract for embedding service backends.
 *
 * Implementations call an external API (OpenAI, Azure OpenAI, HuggingFace TEI)
 * and return L2-normalised float32 vectors. Batching is preferred over single-call
 * `embed` to reduce API round-trips.
 *
 * Available implementations:
 *  - `OpenAIEmbeddingService` — OpenAI or Azure OpenAI REST API.
 *  - `E5EmbeddingService`     — HuggingFace Text Embeddings Inference (TEI) server.
 */
trait EmbeddingService {

  /** Short identifier used in logs and stored as `modelId` on each chunk (e.g. `"openai"`, `"e5"`). */
  def getName: String

  /** Implementation version for observability. */
  def getVersion: String

  /** Dimension count of the output vectors. */
  def getDimensions: Int

  /**
   * Embeds a single text string. Convenience wrapper around [[embedBatch]].
   *
   * @param text Input text.
   * @return Float32 embedding vector of length [[getDimensions]].
   */
  def embed(text: String): Array[Double]

  /**
   * Embeds a batch of texts in a single API call.
   *
   * @param texts Input texts; must not exceed the provider's max batch size.
   * @return Embeddings in the same order as `texts`.
   */
  def embedBatch(texts: List[String]): List[Array[Double]]

  /** Releases any held resources (HTTP client, connections). */
  def close(): Unit
}
