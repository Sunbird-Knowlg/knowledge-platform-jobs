package org.sunbird.job.contentembedding.spec

import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.sunbird.job.contentembedding.domain.ChunkingConfig
import org.sunbird.job.contentembedding.factory.ChunkingStrategyFactory
import org.sunbird.job.contentembedding.strategy.{SemanticChunkingStrategy, SlidingWindowChunkingStrategy}

class ChunkingStrategyFactorySpec extends FlatSpec with BeforeAndAfterAll with Matchers {

  "ChunkingStrategyFactory" should "return SemanticChunkingStrategy for 'semantic'" in {
    val strat = ChunkingStrategyFactory.getStrategy(ChunkingConfig("semantic"))
    strat shouldBe a[SemanticChunkingStrategy]
    strat.getName shouldBe "semantic"
  }

  it should "return SlidingWindowChunkingStrategy for 'sliding-window'" in {
    val strat = ChunkingStrategyFactory.getStrategy(ChunkingConfig("sliding-window"))
    strat shouldBe a[SlidingWindowChunkingStrategy]
    strat.getName shouldBe "sliding-window"
  }

  it should "be case-insensitive for strategy name" in {
    ChunkingStrategyFactory.getStrategy(ChunkingConfig("Semantic")) shouldBe a[SemanticChunkingStrategy]
    ChunkingStrategyFactory.getStrategy(ChunkingConfig("SEMANTIC")) shouldBe a[SemanticChunkingStrategy]
    ChunkingStrategyFactory.getStrategy(ChunkingConfig("Sliding-Window")) shouldBe a[SlidingWindowChunkingStrategy]
  }

  it should "throw IllegalArgumentException for unknown strategy" in {
    assertThrows[IllegalArgumentException] {
      ChunkingStrategyFactory.getStrategy(ChunkingConfig("neural"))
    }
  }
}
