package org.sunbird.job.contentembedding.domain

import java.io.Serializable

/**
 * A text chunk paired with its float32 embedding vector, produced by
 * [[org.sunbird.job.contentembedding.function.EmbeddingFunction]].
 *
 * @param text        Source text that was embedded.
 * @param sourceField Field/section origin (e.g. "metadata", "window_0").
 * @param chunkIndex  Zero-based position within the content object.
 * @param vector      Float32 embedding; not yet quantized.
 * @param wordCount   Word count of the source text.
 * @param modelId     Embedding model identifier (e.g. "openai", "e5").
 */
case class EmbeddedChunk(
    text: String,
    sourceField: String,
    chunkIndex: Int,
    vector: Array[Double],
    wordCount: Int,
    modelId: String
) extends Serializable
