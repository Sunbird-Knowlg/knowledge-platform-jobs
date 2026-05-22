package org.sunbird.job.contentembedding.domain

import java.io.Serializable

/**
 * Int8-quantized embedding vector produced by a [[org.sunbird.job.contentembedding.service.QuantizationStrategy]].
 *
 * For L2-normalised vectors (all OpenAI / E5 outputs): `scale = 127.0`, `offset = 0.0`,
 * and dequantization is `byte / 127.0`.
 * For unnormalised vectors: `scale = (max - min)`, `offset = min` (per-vector min-max fallback).
 *
 * @param chunkIndex         Position of the originating chunk.
 * @param vector             Byte array of quantized values in range [-127, 127].
 * @param quantizationType   Strategy name (e.g. `"int8"`).
 * @param originalDimensions Dimension count before quantization.
 * @param scale              Dequantization scale factor.
 * @param offset             Dequantization offset.
 */
case class QuantizedVector(
    chunkIndex: Int,
    vector: Array[Byte],
    quantizationType: String,
    originalDimensions: Int,
    scale: Double = 1.0,
    offset: Double = 0.0
) extends Serializable
