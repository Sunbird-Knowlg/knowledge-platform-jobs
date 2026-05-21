package org.sunbird.job.contentembedding.domain

import java.io.Serializable

case class VectorEmbedding(
    chunkIndex: Int,
    vector: Array[Double],
    modelId: String,
    dimensions: Int,
    tokenCount: Int
) extends Serializable
