package org.sunbird.job.contentembedding.service

import org.sunbird.job.contentembedding.domain.{VectorEmbedding, QuantizedVector}

trait QuantizationStrategy {

  def getName: String

  def getVersion: String

  def quantize(embedding: VectorEmbedding): QuantizedVector

  def dequantize(quantized: QuantizedVector): Array[Double]
}
