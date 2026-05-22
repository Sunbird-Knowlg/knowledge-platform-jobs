package org.sunbird.job.contentembedding.service

import org.sunbird.job.contentembedding.domain.TextChunk

/**
 * Contract for all text chunking strategies.
 *
 * Implementations split enriched content metadata into a list of [[TextChunk]]s
 * suitable for embedding. Each chunk should be semantically cohesive and within
 * the token limit of the target embedding model.
 *
 * Available implementations:
 *  - `SemanticChunkingStrategy`  — field-based, one chunk per content section.
 *  - `SlidingWindowChunkingStrategy` — overlapping windows over concatenated text.
 */
trait ChunkingStrategy {

  /** Short identifier used in logs and config (e.g. `"semantic"`, `"sliding-window"`). */
  def getName: String

  /** Implementation version for observability. */
  def getVersion: String

  /**
   * Splits enriched content metadata into text chunks.
   *
   * @param objectId    Sunbird content identifier.
   * @param contentType Object type: Content | Collection | Question | QuestionSet.
   * @param data        Enriched metadata fields from the Kafka event.
   * @return Ordered list of chunks; empty if no usable text was found.
   */
  def chunk(
      objectId: String,
      contentType: String,
      data: Map[String, Any]
  ): List[TextChunk]
}
