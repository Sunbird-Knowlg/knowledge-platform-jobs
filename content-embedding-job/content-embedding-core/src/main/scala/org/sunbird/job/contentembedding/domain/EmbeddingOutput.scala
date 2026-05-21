package org.sunbird.job.contentembedding.domain

import java.io.Serializable

case class ChunkEmbedding(
    text: String,
    sourceField: String,
    embedding: Array[Byte],
    index: Int,
    tokenCount: Int
) extends Serializable

case class EmbeddingOutput(
    objectId: String,
    contentType: String,
    chunks: List[ChunkEmbedding],
    embeddingModel: String,
    quantizationType: String,
    timestamp: Long
) extends Serializable
