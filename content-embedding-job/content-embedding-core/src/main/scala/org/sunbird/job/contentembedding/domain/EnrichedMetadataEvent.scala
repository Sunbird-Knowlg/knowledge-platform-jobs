package org.sunbird.job.contentembedding.domain

import java.io.Serializable

case class EnrichedMetadataEvent(
    id: String,
    contentType: String,
    _schema_version: String,
    timestamp: Long,
    data: Map[String, Any]
) extends Serializable
