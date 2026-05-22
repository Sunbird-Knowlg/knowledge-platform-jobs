package org.sunbird.job.contentembedding.service

import org.sunbird.job.contentembedding.domain.{VectorEmbedding, QuantizedVector}

/**
 * Contract for vector quantization strategies.
 *
 * Quantization compresses float32 embeddings to a smaller numeric type before
 * writing to OpenSearch, reducing storage and improving kNN query performance.
 *
 * Available implementations:
 *  - `Int8QuantizationStrategy` — converts float32 to int8 (4× compression,
 *    optimal for L2-normalised vectors from OpenAI / E5).
 */
trait QuantizationStrategy {

  /** Short identifier stored on each quantized vector (e.g. `"int8"`). */
  def getName: String

  /** Implementation version for observability. */
  def getVersion: String

  /**
   * Quantizes a float32 embedding to a compact byte representation.
   *
   * @param embedding Float32 vector from an embedding service.
   * @return Quantized vector with scale/offset for faithful dequantization.
   */
  def quantize(embedding: VectorEmbedding): QuantizedVector

  /**
   * Reconstructs an approximate float32 vector from a quantized representation.
   * Used for debugging and validation; not on the hot path.
   *
   * @param quantized Quantized vector produced by [[quantize]].
   * @return Approximate float32 vector.
   */
  def dequantize(quantized: QuantizedVector): Array[Double]
}
