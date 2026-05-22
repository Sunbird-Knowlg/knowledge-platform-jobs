package org.sunbird.job.contentembedding.factory

import org.slf4j.LoggerFactory
import org.sunbird.job.contentembedding.domain.EmbeddingServiceConfig
import org.sunbird.job.contentembedding.service.{E5EmbeddingService, EmbeddingService, OpenAIEmbeddingService}

/** Creates [[org.sunbird.job.contentembedding.service.EmbeddingService]] instances by name. */
object EmbeddingServiceFactory {

  private[this] val logger = LoggerFactory.getLogger(getClass)

  def getService(config: EmbeddingServiceConfig): EmbeddingService = {
    val service = config.serviceName match {
      case "e5"     => new E5EmbeddingService(config)
      case "openai" => new OpenAIEmbeddingService(config)
      case name     => throw new IllegalArgumentException(
        s"Unknown embedding service: '$name'. Available: e5, openai"
      )
    }
    logger.info(s"Created EmbeddingService: ${service.getName} v${service.getVersion} (${service.getDimensions}d)")
    service
  }
}
