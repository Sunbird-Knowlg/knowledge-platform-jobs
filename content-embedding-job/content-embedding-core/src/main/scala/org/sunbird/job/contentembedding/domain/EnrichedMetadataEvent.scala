package org.sunbird.job.contentembedding.domain

import java.io.Serializable

/**
 * Kafka event consumed from the `enriched.content.metadata` topic.
 *
 * Published by the knowlg-publish pipeline after a content object is fully enriched.
 * Deserialized from JSON by [[org.sunbird.job.contentembedding.function.ExtractFunction]]
 * and routed to [[org.sunbird.job.contentembedding.function.ChunkingFunction]] via
 * the `enrichedOutTag` side output.
 *
 * @param id              Sunbird content identifier (e.g. `do_12345`).
 * @param contentType     Object type: Content | Collection | Question | QuestionSet.
 * @param _schema_version Schema version of the event payload.
 * @param timestamp       Event creation time in epoch milliseconds.
 * @param data            Full enriched metadata fields (name, description, hierarchy, etc.).
 */
case class EnrichedMetadataEvent(
    id: String,
    contentType: String,
    _schema_version: String,
    timestamp: Long,
    data: Map[String, Any]
) extends Serializable
