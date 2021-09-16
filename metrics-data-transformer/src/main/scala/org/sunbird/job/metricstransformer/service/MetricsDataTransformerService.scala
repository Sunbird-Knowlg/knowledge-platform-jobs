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
    var response: Map[String, AnyRef] = Map()
    try {
      response = getContent(url)(config, httpUtil)
      logger.info(s"Response for content $identifier ::" + response)
    } catch {
      case ex: Exception =>
        metrics.incCounter(config.failedEventCount)
        ex.printStackTrace()
        throw new InvalidEventException(s"Error while getting content data from read API for :: $identifier")
    }

    if (!response.isEmpty) {
      val propertyMap = event.transactionData("properties").asInstanceOf[Map[String, AnyRef]]
      var contentMetrics = ""

      keys.foreach(f => {
        val property = ScalaJsonUtil.deserialize[ContentProps](ScalaJsonUtil.serialize(propertyMap(f)))
        if (null != property.nv) {
          contentMetrics = contentMetrics + s""""$f": ${property.nv},""".stripMargin
        }
      })
      val sourcingId = response.getOrElse("origin", "").asInstanceOf[String]
      val originData = response.getOrElse("originData", Map()).asInstanceOf[Map[String, AnyRef]]
      if(event.isValidContent(sourcingId, originData)) {
        val hasOriginContent: Boolean = readOriginContent(sourcingId, metrics, event)(config,httpUtil)
        if(hasOriginContent) {
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
              throw new InvalidEventException(s"Error writing metrics for sourcing id: $sourcingId", Map("partition" -> event.partition, "offset" -> event.offset), ex)
          }
        } else {
          logger.info("Sourcing Read API does not have details for the identifier: " + identifier)
          metrics.incCounter(config.skippedEventCount)
        }
      } else {
        logger.info("Origin and originData of the content is not present in the response of content-read API for id: " + identifier)
        metrics.incCounter(config.skippedEventCount)
      }
    }
    else {
      logger.info(s"Learning event skipped, no sourcing identifier found: $identifier")
      metrics.incCounter(config.skippedEventCount)
    }
  }

  def readOriginContent(sourcingId: String, metrics: Metrics, event: Event)(config: MetricsDataTransformerConfig, httpUtil: HttpUtil): Boolean = {
    val sourcingReadURL = config.lpURL + config.contentReadApi + "/" + sourcingId
    try {
      val response = getContent(sourcingReadURL)(config,httpUtil)
      if (!response.isEmpty) true else false
    } catch {
      case ex: Exception =>
        metrics.incCounter(config.failedEventCount)
        ex.printStackTrace()
        throw new InvalidEventException(s"Error while getting content data from sourcing content read API for :: $sourcingId", Map("partition" -> event.partition, "offset" -> event.offset), ex)
    }
  }

  def getContent(url: String)(config: MetricsDataTransformerConfig, httpUtil: HttpUtil): Map[String, AnyRef] = {
    val response = httpUtil.get(url, config.defaultHeaders)
    if (200 == response.status) {
      ScalaJsonUtil.deserialize[Map[String, AnyRef]](response.body)
        .getOrElse("result", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
        .getOrElse("content", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
    } else if (404 == response.status) {
      Map()
    } else {
      throw new InvalidEventException(s"Error from content read API with response: $response")
    }
  }

  def updateContentMetrics(contentProperty: String, sourcingId: String, channel: String)(config: MetricsDataTransformerConfig, httpUtil: HttpUtil): Boolean = {
    val updateUrl = config.lpURL + config.contentUpdate + "/" + sourcingId
    val contentMetrics = if (contentProperty.nonEmpty) contentProperty.substring(0, contentProperty.length - 1) else contentProperty

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
    val response: HTTPResponse = httpUtil.patch(updateUrl, request, headers)
    if (response.status == 200) {
      true
    } else {
      logger.error("Error while updating metrics for content : " + sourcingId + " :: " + response.body)
      throw new InvalidEventException("Error while updating metrics for content : " + sourcingId + " :: " + response.body)
    }
  }

}
