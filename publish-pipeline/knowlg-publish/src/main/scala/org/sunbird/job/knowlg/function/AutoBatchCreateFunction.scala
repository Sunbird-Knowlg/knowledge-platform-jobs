package org.sunbird.job.knowlg.function

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.knowlg.publish.helpers.AutoBatchCreation
import org.sunbird.job.knowlg.task.KnowlgPublishConfig
import org.sunbird.job.util.HttpUtil
import org.sunbird.job.{BaseProcessFunction, Metrics}

import java.time.format.DateTimeFormatter
import java.time.{ZoneId, ZonedDateTime}

class AutoBatchCreateFunction(config: KnowlgPublishConfig, httpUtil: HttpUtil)
  extends BaseProcessFunction[java.util.Map[String, AnyRef], String](config) with AutoBatchCreation {

  private[this] val logger = LoggerFactory.getLogger(classOf[AutoBatchCreateFunction])

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
  }

  override def close(): Unit = {
    super.close()
  }

  override def processElement(eData: java.util.Map[String, AnyRef], context: ProcessFunction[java.util.Map[String, AnyRef], String]#Context, metrics: Metrics): Unit = {
    val identifier = eData.getOrDefault("identifier", "")
    metrics.incCounter(config.autoBatchCreationCount)
    val startDate = ZonedDateTime.now(ZoneId.of("Asia/Kolkata")).format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
    logger.info("Creating auto batch for " + identifier + " with start date:" + startDate)
    try {
      createBatch(eData, startDate)(config, httpUtil)
      metrics.incCounter(config.autoBatchCreationSuccessCount)
      logger.info("Auto batch created for " + identifier)
    } catch {
      case ex: Throwable =>
        logger.error(s"Error while creating auto batch for identifier : ${identifier}.", ex)
        metrics.incCounter(config.autoBatchCreationFailedCount)
    }
  }

  override def metricsList(): List[String] = {
    List(config.autoBatchCreationCount, config.autoBatchCreationSuccessCount, config.autoBatchCreationFailedCount)
  }

}
