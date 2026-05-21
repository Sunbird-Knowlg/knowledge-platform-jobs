package org.sunbird.job.contentembedding.domain

import java.io.Serializable

case class ChunkedEvent(
    objectId: String,
    contentType: String,
    schemaVersion: String,
    chunks: List[TextChunk]
) extends Serializable
