package org.sunbird.job.contentembedding.factory

import org.slf4j.LoggerFactory
import org.sunbird.job.contentembedding.domain.ChunkingConfig
import org.sunbird.job.contentembedding.service.ChunkingStrategy
import org.sunbird.job.contentembedding.strategy.{SemanticChunkingStrategy, SlidingWindowChunkingStrategy}

/** Creates [[org.sunbird.job.contentembedding.service.ChunkingStrategy]] instances by name. */
object ChunkingStrategyFactory {

  private[this] val logger = LoggerFactory.getLogger(getClass)

  def getStrategy(config: ChunkingConfig): ChunkingStrategy = {
    if (config == null) {
      throw new IllegalArgumentException("ChunkingConfig is null")
    }

    val strategyName = Option(config.strategyName)
      .filter(_.nonEmpty)
      .getOrElse(throw new IllegalArgumentException("strategyName is null or empty"))

    logger.debug(s"Creating chunking strategy: $strategyName")

    val strategy = strategyName.toLowerCase match {
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
