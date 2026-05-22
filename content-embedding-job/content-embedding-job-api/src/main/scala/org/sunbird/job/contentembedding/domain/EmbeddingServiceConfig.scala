package org.sunbird.job.contentembedding.domain

import java.io.Serializable

/**
 * Provider-agnostic configuration for an [[org.sunbird.job.contentembedding.service.EmbeddingService]].
 *
 * Fields are optional and interpreted by each service implementation:
 * - E5 (HuggingFace TEI): `host`, `port`
 * - Standard OpenAI: `apiKey`, `model`
 * - Azure OpenAI: `apiKey`, `model`, `azureEndpoint`, `azureApiVersion`, `azureDeployment`
 *
 * Azure mode is activated automatically when `azureEndpoint` is non-empty.
 *
 * @param serviceName      Service identifier: `"e5"` or `"openai"`.
 * @param dimensions       Output vector dimensions (768 for E5, 1536 for text-embedding-3-small).
 * @param timeoutSeconds   HTTP request timeout.
 * @param host             TEI server hostname (E5 only).
 * @param port             TEI server port (E5 only).
 * @param apiKey           API key (OpenAI) or Azure API key.
 * @param model            Model name / deployment name for OpenAI.
 * @param azureEndpoint    Azure OpenAI resource endpoint, e.g. `https://<resource>.openai.azure.com/`.
 * @param azureApiVersion  Azure OpenAI API version (default `2024-12-01-preview`).
 * @param azureDeployment  Azure deployment name (defaults to `model` value).
 */
case class EmbeddingServiceConfig(
    serviceName: String,
    dimensions: Int                 = 768,
    timeoutSeconds: Int             = 30,
    host: Option[String]            = None,
    port: Option[Int]               = None,
    apiKey: Option[String]          = None,
    model: Option[String]           = None,
    azureEndpoint: Option[String]   = None,
    azureApiVersion: Option[String] = None,
    azureDeployment: Option[String] = None
) extends Serializable
