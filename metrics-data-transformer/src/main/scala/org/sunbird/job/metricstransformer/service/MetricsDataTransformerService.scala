package org.sunbird.job.metricstransformer.service

import java.io.IOException

import org.slf4j.LoggerFactory
import org.sunbird.job.Metrics
import org.sunbird.job.exception.InvalidEventException
import org.sunbird.job.metricstransformer.domain.Event
import org.sunbird.job.metricstransformer.task.MetricsDataTransformerConfig
import org.sunbird.job.util.{ElasticSearchUtil, HTTPResponse, HttpUtil, JSONUtil, ScalaJsonUtil}

case class ContentProps(nv: AnyRef, ov: AnyRef)

trait MetricsDataTransformerService {
  private[this] lazy val logger = LoggerFactory.getLogger(classOf[MetricsDataTransformerService])

  @throws(classOf[InvalidEventException])
  def processEvent(event: Event, metrics: Metrics, keys: Array[String])(implicit config: MetricsDataTransformerConfig, httpUtil: HttpUtil): Unit = {
    val identifier = event.nodeUniqueId
    val url = config.contentServiceBaseUrl + config.contentReadApi + "/" + identifier
    val response = getSourcingId(url)(config, httpUtil)

    if (!response.isEmpty) {
      val propertyMap = event.transactionData("properties").asInstanceOf[Map[String, AnyRef]]
      var contentMetrics = ""

      keys.foreach(f => {
        val property = ScalaJsonUtil.deserialize[ContentProps](ScalaJsonUtil.serialize(propertyMap(f)))
        if(null != property.nv) {
          contentMetrics = contentMetrics + s"""$f: ${property.nv},""".stripMargin
        }
      })

    val sourcingId = response.getOrElse("origin","").asInstanceOf[String]
    updateContentMetrics(contentMetrics, sourcingId)(config, httpUtil)

    logger.info("Updated content metrics: " + identifier)
    try {
      logger.info("Successfully updated content " + identifier)
      metrics.incCounter(config.successEventCount)
    } catch {
      case ex: IOException =>
        logger.error("Error while indexing message :: " + event.getJson + " :: " + ex.getMessage)
        ex.printStackTrace()
        throw new InvalidEventException(ex.getMessage, Map("partition" -> event.partition, "offset" -> event.offset), ex)
      case ex: Exception =>
        logger.error("Error while processing message :: " + event.getJson + " :: ", ex)
        metrics.incCounter(config.failedEventCount)
    }
  }
  else {
      logger.info("Learning event not qualified for audit")
      metrics.incCounter(config.skippedEventCount)
    }

  }

  def getSourcingId(url: String)(config: MetricsDataTransformerConfig, httpUtil: HttpUtil) : Map[String,AnyRef] = {
    val response = httpUtil.get(url, config.defaultHeaders)
    if(200 == response.status) {
      ScalaJsonUtil.deserialize[Map[String, AnyRef]](response.body)
        .getOrElse("result", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
        .getOrElse("content", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
    } else if(404 == response.status) {
      Map()
    } else {
      throw new Exception(s"Error from get API : ${url}, with response: ${response}")
    }
  }

  def updateContentMetrics(contentProperty: String, sourcingId: String)(config: MetricsDataTransformerConfig, httpUtil: HttpUtil): Boolean = {
    val updateUrl = config.lpURL + config.contentV3Update + "/" + sourcingId
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

    val response:HTTPResponse = httpUtil.patch(updateUrl, request,  config.defaultHeaders)
    if(response.status == 200){
      true
    } else {
      logger.error("Error while updating metrics for content : " + sourcingId + " :: " + response.body)
      throw new Exception("Error while updating metrics for content : " + sourcingId + " :: " + response.body)
    }
  }

}
