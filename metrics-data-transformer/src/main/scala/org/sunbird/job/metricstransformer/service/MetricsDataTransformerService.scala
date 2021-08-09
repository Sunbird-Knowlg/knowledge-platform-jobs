package org.sunbird.job.metricstransformer.service

import org.slf4j.LoggerFactory
import org.sunbird.job.Metrics
import org.sunbird.job.exception.InvalidEventException
import org.sunbird.job.metricstransformer.domain.Event
import org.sunbird.job.metricstransformer.task.MetricsDataTransformerConfig
import org.sunbird.job.util.{HTTPResponse, HttpUtil, ScalaJsonUtil}

case class ContentProps(nv: AnyRef, ov: AnyRef)

trait MetricsDataTransformerService {
  private[this] lazy val logger = LoggerFactory.getLogger(classOf[MetricsDataTransformerService])

  @throws(classOf[InvalidEventException])
  def processEvent(event: Event, metrics: Metrics, keys: Array[String])(implicit config: MetricsDataTransformerConfig, httpUtil: HttpUtil): Unit = {
    val identifier = event.nodeUniqueId
    val url = config.contentServiceBaseUrl + config.contentReadApi + "/" + identifier
    val response = getContent(url)(config, httpUtil)
    logger.info(s"Response for content $identifier ::" + response)

    if (!response.isEmpty) {
      val propertyMap = event.transactionData("properties").asInstanceOf[Map[String, AnyRef]]
      var contentMetrics = ""

      keys.foreach(f => {
        val property = ScalaJsonUtil.deserialize[ContentProps](ScalaJsonUtil.serialize(propertyMap(f)))
        if(null != property.nv) {
          contentMetrics = contentMetrics + s""""$f": ${property.nv},""".stripMargin
        }
      })
    val sourcingId = response.getOrElse("origin","").asInstanceOf[String]

    try {
      val channel = event.channel
      updateContentMetrics(contentMetrics, sourcingId, channel)(config, httpUtil)
      logger.info("Updated content metrics: " + identifier)
      metrics.incCounter(config.successEventCount)
    } catch {
      case ex: Exception =>
        logger.error("Error while writing content metrics :: " + event.getJson + " :: ", ex)
        metrics.incCounter(config.failedEventCount)
        ex.printStackTrace()
        throw new InvalidEventException(s"Error writing metrics for sourcing id: $sourcingId")
    }
  }
  else {
      logger.info(s"Learning event skipped, no sourcing identifier found: $identifier")
      metrics.incCounter(config.skippedEventCount)
    }

  }

  def getContent(url: String)(config: MetricsDataTransformerConfig, httpUtil: HttpUtil) : Map[String,AnyRef] = {
    val response = httpUtil.get(url, config.defaultHeaders)
    if(200 == response.status) {
      ScalaJsonUtil.deserialize[Map[String, AnyRef]](response.body)
        .getOrElse("result", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
        .getOrElse("content", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
    } else if(404 == response.status) {
      Map()
    } else {
      throw new InvalidEventException(s"Error from get API with response: $response")
    }
  }

  def updateContentMetrics(contentProperty: String, sourcingId: String, channel: String)(config: MetricsDataTransformerConfig, httpUtil: HttpUtil): Boolean = {
    val updateUrl = config.lpURL + config.contentUpdate + "/" + sourcingId
    val contentMetrics = if(contentProperty.nonEmpty) contentProperty.substring(0,contentProperty.length-1) else contentProperty

    val request =
      s"""
         |{
         |  "request": {
         |    "content": {
         |      $contentMetrics
         |    }
         |  }
         |}""".stripMargin

    val headers = config.defaultHeaders ++ Map("X-Channel-Id" -> channel)
    val response:HTTPResponse = httpUtil.patch(updateUrl, request,  headers)
    if(response.status == 200) {
      true
    } else {
      logger.error("Error while updating metrics for content : " + sourcingId + " :: " + response.body)
      throw new InvalidEventException("Error while updating metrics for content : " + sourcingId + " :: " + response.body)
    }
  }

}
