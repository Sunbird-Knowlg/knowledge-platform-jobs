package org.sunbird.job.contentembedding.domain

import java.io.Serializable

case class EmbeddingServiceConfig(
    serviceName: String,
    dimensions: Int                 = 768,
    timeoutSeconds: Int             = 30,
    // E5-specific
    host: Option[String]            = None,
    port: Option[Int]               = None,
    // OpenAI / Azure OpenAI
    apiKey: Option[String]          = None,
    model: Option[String]           = None,
    // Azure OpenAI — when set, overrides api.openai.com with Azure endpoint + api-key header
    azureEndpoint: Option[String]   = None,
    azureApiVersion: Option[String] = None,
    azureDeployment: Option[String] = None
) extends Serializable
