package org.sunbird.job.contentembedding.domain

import java.io.Serializable

/**
 * A single unit of text produced by a [[org.sunbird.job.contentembedding.service.ChunkingStrategy]].
 *
 * @param text        The chunk text that will be sent to the embedding service.
 * @param sourceField Identifies which field or section the text came from
 *                    (e.g. "metadata", "child_do_123", "window_2").
 * @param index       Zero-based position of this chunk within the content object.
 * @param metadata    Strategy-specific key/value annotations (e.g. window start/end, parent id).
 */
case class TextChunk(
    text: String,
    sourceField: String,
    index: Int,
    metadata: Map[String, Any] = Map()
) extends Serializable
