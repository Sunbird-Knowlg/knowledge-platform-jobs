package org.sunbird.job.contentembedding.domain

import java.io.Serializable

/**
 * Raw float32 embedding vector returned by an [[org.sunbird.job.contentembedding.service.EmbeddingService]].
 * Passed to a [[org.sunbird.job.contentembedding.service.QuantizationStrategy]] for compression.
 *
 * @param chunkIndex  Position of the originating chunk within the content object.
 * @param vector      Float32 embedding values; length == `dimensions`.
 * @param modelId     Identifier of the embedding model (e.g. "openai", "e5").
 * @param dimensions  Vector length (1536 for text-embedding-3-small, 768 for multilingual-e5-large).
 * @param tokenCount  Approximate token count of the source text (word-count proxy).
 */
case class VectorEmbedding(
    chunkIndex: Int,
    vector: Array[Double],
    modelId: String,
    dimensions: Int,
    tokenCount: Int
) extends Serializable
