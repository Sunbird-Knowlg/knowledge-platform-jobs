package org.sunbird.job.contentembedding.factory

import org.slf4j.LoggerFactory
import org.sunbird.job.contentembedding.domain.ChunkingConfig
import org.sunbird.job.contentembedding.service.ChunkingStrategy
import org.sunbird.job.contentembedding.strategy.{SemanticChunkingStrategy, SlidingWindowChunkingStrategy}

object ChunkingStrategyFactory {

  private[this] val logger = LoggerFactory.getLogger(getClass)

  def getStrategy(config: ChunkingConfig): ChunkingStrategy = {
    val strategy = config.strategyName match {
      case "semantic"       => new SemanticChunkingStrategy(config)
      case "sliding-window" => new SlidingWindowChunkingStrategy(config)
      case name             => throw new IllegalArgumentException(
        s"Unknown chunking strategy: '$name'. Available: semantic, sliding-window"
      )
    }
    logger.info(s"Created ChunkingStrategy: ${strategy.getName} v${strategy.getVersion}")
    strategy
  }
}
