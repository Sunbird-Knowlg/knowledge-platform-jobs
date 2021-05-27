package org.sunbird.job.mvcindexer.util

import org.slf4j.LoggerFactory
import org.sunbird.job.mvcindexer.task.MVCProcessorIndexerConfig


object ContentUtil {
  private[this] lazy val logger = LoggerFactory.getLogger(ContentUtil.getClass)

  @throws[Exception]
  def getContentMetaData(newmap: Map[String, AnyRef], identifer: String, httpUtil: HttpUtil, config: MVCProcessorIndexerConfig): Map[String, AnyRef] = {
    try {
      val contentReadURL = config.contentServiceBase
      logger.info("MVCProcessorCassandraIndexer :: getContentMetaData :::  Making API call to read content " + contentReadURL + "/content/v3/read/")
      val content:HTTPResponse = httpUtil.get(contentReadURL + "/content/v3/read/" + identifer)
      logger.info("MVCProcessorCassandraIndexer :: getContentMetaData ::: retrieved content meta " + content)
      val obj = JSONUtil.deserialize[Map[String, AnyRef]](content.body)
      val contentobj = (obj.get("result").asInstanceOf[Map[String, AnyRef]]).get("content").asInstanceOf[Map[String, AnyRef]]
      filterData(newmap, contentobj)
    } catch {
      case e: Exception =>
        logger.info("MVCProcessorCassandraIndexer :: getContentDefinition ::: Error in getContentDefinitionFunction " + e.getMessage)
        throw e
    }
  }

  def filterData(obj: Map[String, AnyRef], content: Map[String, AnyRef]): Map[String, AnyRef] = {
    val elasticSearchParamArr = Array("organisation", "channel", "framework", "board", "medium", "subject", "gradeLevel", "name", "description", "language", "appId", "appIcon", "appIconLabel", "contentEncoding", "identifier", "node_id", "nodeType", "mimeType", "resourceType", "contentType", "allowedContentTypes", "objectType", "posterImage", "artifactUrl", "launchUrl", "previewUrl", "streamingUrl", "downloadUrl", "status", "pkgVersion", "source", "lastUpdatedOn", "ml_contentText", "ml_contentTextVector", "ml_Keywords", "level1Name", "level1Concept", "level2Name", "level2Concept", "level3Name", "level3Concept", "textbook_name", "sourceURL", "label", "all_fields")
    var key:String = null
    var value:AnyRef = null
    for (i <- 0 until elasticSearchParamArr.length) {
      key = elasticSearchParamArr(i)
      value = if (content.contains(key)) content.get(key)
      else null
      if (value != null) {
        obj.put(key, value)
        value = null
      }
    }
    obj
  }
}
