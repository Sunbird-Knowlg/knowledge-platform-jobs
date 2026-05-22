package org.sunbird.job.contentembedding.domain

import java.io.Serializable

/**
 * Configuration for a [[org.sunbird.job.contentembedding.service.QuantizationStrategy]].
 *
 * @param strategyName Strategy to use. Currently supported: `"int8"`.
 */
case class QuantizationConfig(
    strategyName: String
) extends Serializable
