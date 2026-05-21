package org.sunbird.job.contentembedding.factory

import org.slf4j.LoggerFactory
import org.sunbird.job.contentembedding.domain.QuantizationConfig
import org.sunbird.job.contentembedding.service.QuantizationStrategy
import org.sunbird.job.contentembedding.strategy.Int8QuantizationStrategy

object QuantizationStrategyFactory {

  private[this] val logger = LoggerFactory.getLogger(getClass)

  def getStrategy(config: QuantizationConfig): QuantizationStrategy = {
    val strategy = config.strategyName match {
      case "int8" => new Int8QuantizationStrategy()
      case name   => throw new IllegalArgumentException(
        s"Unknown quantization strategy: '$name'. Available: int8"
      )
    }
    logger.info(s"Created QuantizationStrategy: ${strategy.getName} v${strategy.getVersion}")
    strategy
  }
}
