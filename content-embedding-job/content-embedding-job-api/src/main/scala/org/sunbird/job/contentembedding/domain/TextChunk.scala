package org.sunbird.job.contentembedding.domain

import java.io.Serializable

case class TextChunk(
    text: String,
    sourceField: String,
    index: Int,
    metadata: Map[String, Any] = Map()
) extends Serializable
