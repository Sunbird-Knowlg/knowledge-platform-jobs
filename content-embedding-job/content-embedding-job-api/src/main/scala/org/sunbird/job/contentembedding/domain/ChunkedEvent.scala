package org.sunbird.job.contentembedding.domain

import java.io.Serializable

/**
 * Pipeline event emitted by [[org.sunbird.job.contentembedding.function.ChunkingFunction]]
 * after splitting an enriched content object into text chunks.
 *
 * Routed via the `chunkedOutTag` side output to [[org.sunbird.job.contentembedding.function.EmbeddingFunction]].
 *
 * @param objectId      Sunbird content identifier (e.g. `do_12345`).
 * @param contentType   Object type: Content | Collection | Question | QuestionSet.
 * @param schemaVersion Schema version from the source event.
 * @param chunks        Ordered list of text chunks ready for embedding.
 */
case class ChunkedEvent(
    objectId: String,
    contentType: String,
    schemaVersion: String,
    chunks: List[TextChunk]
) extends Serializable
