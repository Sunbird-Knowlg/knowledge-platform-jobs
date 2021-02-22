package org.sunbird.job.functions

import java.time.format.DateTimeFormatter
import java.time.{ZoneId, ZonedDateTime}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.postpublish.helpers.BatchCreation
import org.sunbird.job.{BaseProcessFunction, Metrics}
import org.sunbird.job.task.PostPublishProcessorConfig
import org.sunbird.job.util.HttpUtil

class BatchCreateFunction(config: PostPublishProcessorConfig, httpUtil: HttpUtil)
  extends BaseProcessFunction[java.util.Map[String, AnyRef], String](config) with BatchCreation {

  private[this] val logger = LoggerFactory.getLogger(classOf[BatchCreateFunction])

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
  }

  override def close(): Unit = {
    super.close()
  }

  override def processElement(eData: java.util.Map[String, AnyRef], context: ProcessFunction[java.util.Map[String, AnyRef], String]#Context, metrics: Metrics): Unit = {
    if(eData.isEmpty) return
    val collectionId = eData.getOrDefault("identifier", "")
    val startDate = ZonedDateTime.now(ZoneId.of("Asia/Kolkata")).format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
    logger.info("Creating Batch for " + collectionId + " with start date:" + startDate)
    createBatch(eData, startDate)(config, httpUtil)
    metrics.incCounter(config.batchCreationCount)
    logger.info("Batch created for " + collectionId)
  }

  override def metricsList(): List[String] = {
    List(config.batchCreationCount)
  }

}
