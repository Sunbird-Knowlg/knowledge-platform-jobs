package org.sunbird.job.contentembedding.strategy

import org.slf4j.LoggerFactory
import org.sunbird.job.contentembedding.domain.{QuantizedVector, VectorEmbedding}
import org.sunbird.job.contentembedding.service.QuantizationStrategy

/**
 * Int8 (byte) quantization strategy for float32 embedding vectors.
 *
 * Two paths based on the vector's L2 norm:
 *
 * '''L2-normalised path''' (norm ≈ 1.0, tolerance 0.01) — used for all OpenAI and E5 outputs:
 *  - `byte = round(clamp(v × 127, -127, 127))`
 *  - `scale = 127.0`, `offset = 0.0`
 *  - Dequantize: `v ≈ byte / 127.0`
 *  - 4× compression, &lt;2% cosine similarity loss on MTEB benchmarks.
 *
 * '''Unnormalised fallback''' (per-vector min-max scaling):
 *  - Maps `[min, max]` → `[-128, 127]`
 *  - `scale = (max - min)`, `offset = min`
 *  - Slightly lower precision but lossless range coverage.
 */
class Int8QuantizationStrategy extends QuantizationStrategy {

  private[this] val logger = LoggerFactory.getLogger(classOf[Int8QuantizationStrategy])

  override def getName: String = "int8"

  override def getVersion: String = "2.0"

  override def quantize(embedding: VectorEmbedding): QuantizedVector = {
    val vector  = embedding.vector
    val l2Norm  = math.sqrt(vector.map(v => v * v).sum)
    val isNormalized = math.abs(l2Norm - 1.0) < 0.01

    val (quantized, scale, offset) = if (isNormalized) {
      // L2-normalized: values guaranteed in [-1, 1], use global scale
      // byte = round(v * 127), dequantize: v = byte / 127.0
      val bytes = vector.map(v => math.round(math.max(-127, math.min(127, v * 127))).toByte)
      logger.debug(s"Chunk ${embedding.chunkIndex}: L2-normalized, global scale 127")
      (bytes, 127.0, 0.0)
    } else {
      // Unnormalized: per-vector min-max fallback
      val minVal = vector.min
      val maxVal = vector.max
      val range  = maxVal - minVal

      val bytes = if (range == 0) {
        Array.fill(vector.length)(0.toByte)
      } else {
        vector.map { v =>
          val normalized = (v - minVal) / range   // [0, 1]
          val scaled     = normalized * 255 - 128 // [-128, 127]
          math.round(math.max(-128, math.min(127, scaled))).toByte
        }
      }
      logger.debug(s"Chunk ${embedding.chunkIndex}: unnormalized (norm=$l2Norm), per-vector min-max")
      (bytes, range, minVal)
    }

    QuantizedVector(
      chunkIndex         = embedding.chunkIndex,
      vector             = quantized,
      quantizationType   = getName,
      originalDimensions = embedding.dimensions,
      scale              = scale,
      offset             = offset
    )
  }

  override def dequantize(quantized: QuantizedVector): Array[Double] = {
    if (quantized.scale == 127.0 && quantized.offset == 0.0) {
      quantized.vector.map(_.toDouble / 127.0)
    } else if (quantized.scale == 0.0) {
      Array.fill(quantized.vector.length)(quantized.offset)
    } else {
      quantized.vector.map { byte =>
        val normalized = (byte.toDouble + 128) / 255.0
        normalized * quantized.scale + quantized.offset
      }
    }
  }
}
