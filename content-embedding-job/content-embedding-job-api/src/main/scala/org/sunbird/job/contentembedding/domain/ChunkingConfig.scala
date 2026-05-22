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
 */
case class ChunkingConfig(
    strategyName: String,
    maxChunkSize: Int   = 1000,
    maxTokens: Int      = 512,
    overlapTokens: Int  = 102
) extends Serializable
