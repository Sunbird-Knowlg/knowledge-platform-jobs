package org.sunbird.job.contentembedding.domain

import java.io.Serializable

case class EmbeddingServiceConfig(
    serviceName: String,
    dimensions: Int         = 768,
    timeoutSeconds: Int     = 30,
    // E5-specific
    host: Option[String]    = None,
    port: Option[Int]       = None,
    // OpenAI-specific
    apiKey: Option[String]  = None,
    model: Option[String]   = None
) extends Serializable
