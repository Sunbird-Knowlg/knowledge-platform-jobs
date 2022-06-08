package org.sunbird.job.postpublish.functions

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.postpublish.helpers.DialHelper
import org.sunbird.job.postpublish.task.PostPublishProcessorConfig
import org.sunbird.job.util.ScalaJsonUtil
import org.sunbird.job.{BaseProcessFunction, Metrics}


class DialCodeContextUpdaterFunction(config: PostPublishProcessorConfig) (implicit val stringTypeInfo: TypeInformation[String])
  extends BaseProcessFunction[java.util.Map[String, AnyRef], String](config)
    with DialHelper {

  private[this] val logger = LoggerFactory.getLogger(classOf[DialCodeContextUpdaterFunction])

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
  }

  override def close(): Unit = {
    super.close()
  }

  override def processElement(edata: java.util.Map[String, AnyRef], context: ProcessFunction[java.util.Map[String, AnyRef], String]#Context, metrics: Metrics): Unit = {
    logger.info(s"DIAL Code Context Updater operation triggered with object : $edata")
    metrics.incCounter(config.dialcodeContextUpdaterCount)
    try {
      val addContextDialCodes: Map[List[String],String] = edata.getOrDefault("addContextDialCodes", Map.empty[String,String]).asInstanceOf[Map[String,String]].map(rec => (ScalaJsonUtil.deserialize[List[String]](rec._1)->rec._2))
      val removeContextDialCodes: Map[List[String],String] = edata.getOrDefault("removeContextDialCodes", Map.empty[String,String]).asInstanceOf[Map[String,String]].map(rec => (ScalaJsonUtil.deserialize[List[String]](rec._1)->rec._2))
      val channel: String = edata.getOrDefault("channel", "").asInstanceOf[String]
      generateDialcodeContextUpdaterEvent(channel, addContextDialCodes, removeContextDialCodes, context, metrics)(config)
      metrics.incCounter(config.dialcodeContextUpdaterSuccessCount)
    } catch {
      case ex: Throwable =>
        logger.error(s"Error while processing message for DIAL Code Context Updater  : $edata.", ex)
        metrics.incCounter(config.dialcodeContextUpdaterFailedCount)
        throw ex
    }
  }



  override def metricsList(): List[String] = {
    List(config.dialcodeContextUpdaterCount, config.dialcodeContextUpdaterSuccessCount, config.dialcodeContextUpdaterFailedCount)
  }
}
