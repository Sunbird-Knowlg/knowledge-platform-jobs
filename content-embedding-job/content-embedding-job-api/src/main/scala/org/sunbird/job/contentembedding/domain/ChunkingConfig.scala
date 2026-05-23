package org.sunbird.job.contentembedding.domain

import java.io.Serializable

/**
 * Configuration for a [[org.sunbird.job.contentembedding.service.ChunkingStrategy]].
 *
 * @param strategyName  Strategy to use: `"semantic"` or `"sliding-window"`.
 * @param maxChunkSize  `semantic` only — max characters per chunk before truncation (default 1000).
 * @param maxTokens     `sliding-window` only — max words per window, used as a token proxy (default 512,
 *                      matching the E5/OpenAI token limit).
 * @param overlapTokens `sliding-window` only — words shared between consecutive windows (default 102 ≈ 20%).
 *                      Overlap preserves sentence context at chunk boundaries.
 * @param excludedFields `semantic` only — field names to exclude from dynamic metadata extraction
 *                       (default: hierarchy, children, id, identifier, contentType, _schema_version, timestamp).
 */
case class ChunkingConfig(
    strategyName: String,
    maxChunkSize: Int   = 1000,
    maxTokens: Int      = 512,
    overlapTokens: Int  = 102,
    excludedFields: Set[String] = Set("hierarchy", "children", "id", "identifier", "contentType", "_schema_version", "timestamp")
) extends Serializable
