package org.sunbird.job.contentembedding.domain

import java.io.Serializable

case class ChunkingConfig(
    strategyName: String,
    maxChunkSize: Int   = 1000,  // semantic: max characters per chunk
    maxTokens: Int      = 512,   // sliding-window: max words (token proxy) per window
    overlapTokens: Int  = 102    // sliding-window: overlap words between windows (20% of 512)
) extends Serializable
