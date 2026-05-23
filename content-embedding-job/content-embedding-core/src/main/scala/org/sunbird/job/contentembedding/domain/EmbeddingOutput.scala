package org.sunbird.job.contentembedding.domain

import java.io.Serializable

/**
 * A single int8-quantized chunk ready to be written to OpenSearch.
 *
 * @param text        Source text of the chunk (stored in the `chunks.text` field).
 * @param sourceField Field/section origin (e.g. "metadata", "window_0").
 * @param embedding   Int8-quantized vector as a byte array; stored as `chunks.embedding` (knn_vector).
 * @param index       Zero-based chunk position within the content object.
 * @param wordCount   Word count of the source text.
 */
case class ChunkEmbedding(
    text: String,
    sourceField: String,
    embedding: Array[Byte],
    index: Int,
    wordCount: Int
) extends Serializable

/**
 * Final pipeline output emitted by [[org.sunbird.job.contentembedding.function.QuantizationFunction]].
 *
 * Written to OpenSearch by [[org.sunbird.job.contentembedding.function.OpenSearchSinkFunction]]
 * as a partial update on the `compositesearch` index document keyed by `objectId`.
 * All chunks are stored under the nested `chunks` field on a single document.
 *
 * @param objectId        Sunbird content identifier — used as the OpenSearch document ID.
 * @param contentType     Object type: Content | Collection | Question | QuestionSet.
 * @param chunks          All quantized chunks for this content object.
 * @param embeddingModel  Model that produced the vectors (e.g. "openai", "e5").
 * @param quantizationType Compression strategy applied (e.g. "int8").
 * @param timestamp       Processing time in epoch milliseconds.
 */
case class EmbeddingOutput(
    objectId: String,
    contentType: String,
    chunks: List[ChunkEmbedding],
    embeddingModel: String,
    quantizationType: String,
    timestamp: Long,
    schemaVersion: String = "1.0"
) extends Serializable
