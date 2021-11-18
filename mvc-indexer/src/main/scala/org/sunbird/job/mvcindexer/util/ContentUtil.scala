package org.sunbird.job.mvcindexer.util

import org.sunbird.job.exception.APIException
import org.sunbird.job.mvcindexer.task.MVCIndexerConfig
import org.sunbird.job.util.{HTTPResponse, HttpUtil, JSONUtil}

object ContentUtil {

  /**
   * Get the Content Metadata and return the parsed metadata map
   *
   * @param newmap    Content metadata from event envelope
   * @param identifer Content ID
   * @param httpUtil  HttpUil instance
   * @param config    Config instance
   * @return parsed metadata map
   */
  @throws[Exception]
  def getContentMetaData(newmap: Map[String, AnyRef], identifer: String, httpUtil: HttpUtil, config: MVCIndexerConfig): Map[String, AnyRef] = {
    try {
      val content: HTTPResponse = httpUtil.get(config.contentReadURL + identifer)
      val obj = JSONUtil.deserialize[Map[String, AnyRef]](content.body)
      val contentObj = obj("result").asInstanceOf[Map[String, AnyRef]]("content").asInstanceOf[Map[String, AnyRef]]
      filterData(newmap, contentObj, config)
    } catch {
      case e: Exception =>
        throw new APIException(s"Error in getContentMetaData for $identifer - ${e.getLocalizedMessage}", e)
    }
  }

  def filterData(obj: Map[String, AnyRef], content: Map[String, AnyRef], config: MVCIndexerConfig): Map[String, AnyRef] = {
    var contentObj = JSONUtil.deserialize[Map[String, AnyRef]](JSONUtil.serialize(obj))

    for (param <- config.elasticSearchParamSet) {
      val value = content.getOrElse(param, null)
      if (value != null) {
        contentObj += (param -> value)
      }
    }
    contentObj
  }
}
