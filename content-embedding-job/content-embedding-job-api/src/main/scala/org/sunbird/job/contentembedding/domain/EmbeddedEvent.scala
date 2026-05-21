package org.sunbird.job.contentembedding.domain

import java.io.Serializable

case class EmbeddedEvent(
    objectId: String,
    contentType: String,
    schemaVersion: String,
    chunks: List[EmbeddedChunk]
) extends Serializable
