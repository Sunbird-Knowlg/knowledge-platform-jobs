package org.sunbird.job.mvcindexer.util

import org.slf4j.LoggerFactory
import org.sunbird.job.mvcindexer.task.MVCIndexerConfig
import org.sunbird.job.util.{HTTPResponse, HttpUtil, JSONUtil}
import scala.collection.mutable.{Map => MutableMap}


object ContentUtil {
  private[this] lazy val logger = LoggerFactory.getLogger(ContentUtil.getClass)

  @throws[Exception]
  def getContentMetaData(newmap: Map[String, AnyRef], identifer: String, httpUtil: HttpUtil, config: MVCIndexerConfig): Map[String, AnyRef] = {
    try {
      val contentReadURL = config.contentServiceBase
      logger.info("getContentMetaData :::  Making API call to read content " + contentReadURL + "/content/v3/read/")
      val content:HTTPResponse = httpUtil.get(contentReadURL + "/content/v3/read/" + identifer)
      logger.info("getContentMetaData ::: retrieved content meta " + content)
      val obj = JSONUtil.deserialize[Map[String, AnyRef]](content.body)
      val contentobj = obj.get("result").asInstanceOf[Map[String, AnyRef]].get("content").asInstanceOf[Map[String, AnyRef]]
      filterData(newmap.asInstanceOf[MutableMap[String, AnyRef]], contentobj).asInstanceOf[Map[String, AnyRef]]
    } catch {
      case e: Exception =>
        logger.error("Error in getContentMetaData ", e)
        throw e
    }
  }

  def filterData(obj: MutableMap[String, AnyRef], content: Map[String, AnyRef]): MutableMap[String, AnyRef] = {
    val elasticSearchParamSet = Set("organisation", "channel", "framework", "board", "medium", "subject", "gradeLevel", "name", "description", "language", "appId", "appIcon", "appIconLabel", "contentEncoding", "identifier", "node_id", "nodeType", "mimeType", "resourceType", "contentType", "allowedContentTypes", "objectType", "posterImage", "artifactUrl", "launchUrl", "previewUrl", "streamingUrl", "downloadUrl", "status", "pkgVersion", "source", "lastUpdatedOn", "ml_contentText", "ml_contentTextVector", "ml_Keywords", "level1Name", "level1Concept", "level2Name", "level2Concept", "level3Name", "level3Concept", "textbook_name", "sourceURL", "label", "all_fields")
    var key:String = null
    var value:AnyRef = null
    for (param <- elasticSearchParamSet) {
      value = if (content.contains(param)) content.get(param)
      else null
      if (value != null) {
        obj += (param -> value)
//        obj ++= Map(param -> value)
      }
    }
    obj
  }
}
