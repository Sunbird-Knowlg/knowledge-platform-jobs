package org.sunbird.job.contentembedding.service

trait EmbeddingService {

  def getName: String

  def getVersion: String

  def getDimensions: Int

  def embed(text: String): Array[Double]

  def embedBatch(texts: List[String]): List[Array[Double]]

  def close(): Unit
}
