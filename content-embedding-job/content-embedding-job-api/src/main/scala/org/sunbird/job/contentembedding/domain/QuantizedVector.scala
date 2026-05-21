package org.sunbird.job.contentembedding.domain

import java.io.Serializable

case class QuantizedVector(
    chunkIndex: Int,
    vector: Array[Byte],
    quantizationType: String,
    originalDimensions: Int,
    scale: Double = 1.0,
    offset: Double = 0.0
) extends Serializable
