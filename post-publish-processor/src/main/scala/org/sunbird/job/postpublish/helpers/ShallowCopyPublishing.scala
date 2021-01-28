package org.sunbird.job.postpublish.helpers

import org.apache.commons.lang3.StringUtils
import org.sunbird.job.functions.PublishMetadata
import org.sunbird.job.task.PostPublishProcessorConfig
import org.sunbird.job.util.{HttpUtil, JSONUtil}

import scala.collection.JavaConverters._

trait ShallowCopyPublishing{

  def getShallowCopiedContents(identifier: String)(implicit config: PostPublishProcessorConfig, httpUtil: HttpUtil): List[PublishMetadata] = {
    val httpRequest = s"""{"request":{"filters":{"status":["Draft","Review","Live","Unlisted","Failed"],"origin":"${identifier}"},"fields":["identifier","mimeType","contentType","versionKey","channel","status","pkgVersion","lastPublishedBy","origin","originData"]}}"""
    val httpResponse = httpUtil.post(config.searchAPIPath, httpRequest)
    if (httpResponse.status == 200) {

      val response = JSONUtil.deserialize[Map[String, AnyRef]](httpResponse.body)
      val result = response.getOrElse("result", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
      val contents = result.getOrElse("content", List[Map[String,AnyRef]]()).asInstanceOf[List[Map[String,AnyRef]]]
     contents.filter(c => c.contains("originData"))
        .filter(content => {
          val originDataStr = content.getOrElse("originData", "{}").asInstanceOf[String]
          val originData = JSONUtil.deserialize[Map[String, AnyRef]](originDataStr)
          val copyType = originData.getOrElse("copyType", "deep").asInstanceOf[String]
          (StringUtils.equalsIgnoreCase(copyType, "shallow"))
        }).map(content => {
        val copiedId = content("identifier").asInstanceOf[String]
        val copiedMimeType = content("mimeType").asInstanceOf[String]
        val copiedPKGVersion = content.getOrElse("pkgVersion", 0.asInstanceOf[AnyRef]).asInstanceOf[Number]
        val copiedContentType = content("contentType").asInstanceOf[String]
        PublishMetadata(copiedId, copiedContentType, copiedMimeType, copiedPKGVersion.intValue())
      })
    } else {
      throw new Exception("Content search failed for shallow copy check:" + identifier)
    }
  }
}
