package org.sunbird.job.contentembedding.domain

import java.io.Serializable

case class EmbeddedChunk(
    text: String,
    sourceField: String,
    chunkIndex: Int,
    vector: Array[Double],
    tokenCount: Int,
    modelId: String
) extends Serializable
