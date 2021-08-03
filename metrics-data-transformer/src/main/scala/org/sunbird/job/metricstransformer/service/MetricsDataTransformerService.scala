package org.sunbird.job.metricstransformer.service

import org.slf4j.LoggerFactory
import org.sunbird.job.Metrics
import org.sunbird.job.exception.InvalidEventException
import org.sunbird.job.metricstransformer.domain.Event
import org.sunbird.job.metricstransformer.task.MetricsDataTransformerConfig
import org.sunbird.job.util.{HTTPResponse, HttpUtil, JSONUtil}
import org.springframework.http.HttpStatus

case class ContentProps(nv: AnyRef, ov: AnyRef)

trait MetricsDataTransformerService {
  private[this] lazy val logger = LoggerFactory.getLogger(classOf[MetricsDataTransformerService])

  @throws(classOf[InvalidEventException])
  def processEvent(event: Event, metrics: Metrics, keys: Array[String])(implicit config: MetricsDataTransformerConfig, httpUtil: HttpUtil): Unit = {
    val identifier = event.nodeUniqueId
    val url = config.contentServiceBaseUrl + config.contentReadApi + "/" + identifier
    val response = getContent(url, event)(config, httpUtil)

    if (!response.isEmpty) {
      val propertyMap = event.transactionData("properties").asInstanceOf[Map[String, AnyRef]]
      var contentMetrics = ""

      keys.foreach(f => {
        val property = JSONUtil.deserialize[ContentProps](JSONUtil.serialize(propertyMap(f)))
        if (null != property.nv) {
          contentMetrics = contentMetrics + s"""$f: ${property.nv},""".stripMargin
        }
      })
      val sourcingId = response.getOrElse("origin", "").asInstanceOf[String]

      try {
        val channel = event.channel
        updateContentMetrics(contentMetrics, sourcingId, channel, event)(config, httpUtil)
        logger.info("Updated content metrics: " + identifier)
        metrics.incCounter(config.successEventCount)
      } catch {
        case ex: Exception =>
          logger.error("Error while writing content metrics :: " + event.getJson + " :: ", ex)
          metrics.incCounter(config.failedEventCount)
          ex.printStackTrace()
          throw new InvalidEventException(s"Error writing metrics for sourcing id: $sourcingId", Map("partition" -> event.partition, "offset" -> event.offset), ex)
      }
    }
    else {
      logger.info("Learning event skipped, no sourcing identifier found")
      metrics.incCounter(config.skippedEventCount)
    }

  }

  def getContent(url: String, event: Event)(config: MetricsDataTransformerConfig, httpUtil: HttpUtil): Map[String, AnyRef] = {
    val response = httpUtil.get(url, config.defaultHeaders)

    if (HttpStatus.OK.value() == response.status) {
      JSONUtil.deserialize[Map[String, AnyRef]](response.body)
        .getOrElse("result", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
        .getOrElse("content", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
    } else if (HttpStatus.NOT_FOUND.value() == response.status) {
      Map()
    } else {
      throw new InvalidEventException(s"Error from get API with response: $response; partition -> ${event.partition}, offset -> ${event.offset}")
    }
  }

  def updateContentMetrics(contentProperty: String, sourcingId: String, channel: String, event: Event)(config: MetricsDataTransformerConfig, httpUtil: HttpUtil): Unit = {
    val updateUrl = config.lpURL + config.contentUpdate + "/" + sourcingId
    if (contentProperty.nonEmpty) {
      val contentMetrics = contentProperty.substring(0, contentProperty.length - 1)
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
      if (HttpStatus.OK.value() == response.status) {
        logger.info(s"Updated metrics for $sourcingId")
      } else {
        logger.error("Error while updating metrics for content : " + sourcingId + " :: " + response.body)
        throw new InvalidEventException("Error while updating metrics for content : " + sourcingId + " :: " + response.body + s" :: partition -> ${event.partition}, offset -> ${event.offset}")
      }
    }
    else logger.info(s"No metrics to update for $sourcingId")
  }

}
