package org.sunbird.job.contentembedding.domain

import java.io.Serializable

/**
 * Pipeline event emitted by [[org.sunbird.job.contentembedding.function.EmbeddingFunction]]
 * after generating float32 vectors for all chunks of a content object.
 *
 * Routed via the `embeddedOutTag` side output to [[org.sunbird.job.contentembedding.function.QuantizationFunction]].
 *
 * @param objectId      Sunbird content identifier.
 * @param contentType   Object type: Content | Collection | Question | QuestionSet.
 * @param schemaVersion Schema version carried from the source event.
 * @param chunks        Chunks paired with their raw float32 embedding vectors.
 */
case class EmbeddedEvent(
    objectId: String,
    contentType: String,
    schemaVersion: String,
    chunks: List[EmbeddedChunk]
) extends Serializable
