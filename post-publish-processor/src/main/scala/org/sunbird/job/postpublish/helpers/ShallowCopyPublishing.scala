package org.sunbird.job.postpublish.helpers

import org.apache.commons.lang3.StringUtils
import org.sunbird.job.functions.PublishMetadata
import org.sunbird.job.task.PostPublishProcessorConfig
import org.sunbird.job.util.{HttpUtil, JSONUtil}

import scala.collection.JavaConverters._

trait ShallowCopyPublishing{

  def getShallowCopiedContents(identifier: String)(implicit config: PostPublishProcessorConfig, httpUtil: HttpUtil): List[PublishMetadata] = {
    val httpRequest = s"""{"request":{"filters":{"status":["Draft","Review","Live","Unlisted","Failed"],"origin":"${identifier}"},"fields":["identifier","mimeType","contentType","versionKey","channel","status","pkgVersion","lastPublishedBy","origin","originData"]}}"""
    val httpResponse = httpUtil.post(config.searchBaseUrl + "/v3/search", httpRequest)
    if (httpResponse.status == 200) {

      val response = JSONUtil.deserialize[java.util.Map[String, AnyRef]](httpResponse.body)
      val result = response.getOrDefault("result", new java.util.HashMap[String, AnyRef]()).asInstanceOf[java.util.Map[String, AnyRef]]
      val contents = result.getOrDefault("content", new java.util.ArrayList[java.util.Map[String, AnyRef]]()).asInstanceOf[java.util.List[java.util.Map[String, AnyRef]]]
      contents.asScala.filter(c => c.containsKey("originData"))
        .filter(c => {
          val originDataStr = c.getOrDefault("originData", "{}").asInstanceOf[String]
          val originData = JSONUtil.deserialize[java.util.Map[String, AnyRef]](originDataStr)
          val copyType = originData.getOrDefault("copyType", "deep").asInstanceOf[String]
          (StringUtils.equalsIgnoreCase(copyType, "shallow"))
        }).map(c => {
        val copiedId = c.get("identifier").asInstanceOf[String]
        val copiedMimeType = c.get("mimeType").asInstanceOf[String]
        val copiedPKGVersion = c.getOrDefault("pkgVersion", 0.asInstanceOf[AnyRef]).asInstanceOf[Number]
        val copiedContentType = c.get("contentType").asInstanceOf[String]
        PublishMetadata(copiedId, copiedContentType, copiedMimeType, copiedPKGVersion.intValue())
      }).toList
    } else {
      throw new Exception("Content search failed for shallow copy check:" + identifier)
    }
  }
}
