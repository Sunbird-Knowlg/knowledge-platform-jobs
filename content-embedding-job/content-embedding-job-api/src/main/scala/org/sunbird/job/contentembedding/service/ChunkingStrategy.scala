package org.sunbird.job.contentembedding.service

import org.sunbird.job.contentembedding.domain.TextChunk

trait ChunkingStrategy {

  def getName: String

  def getVersion: String

  def chunk(
      objectId: String,
      contentType: String,
      data: Map[String, Any]
  ): List[TextChunk]
}
