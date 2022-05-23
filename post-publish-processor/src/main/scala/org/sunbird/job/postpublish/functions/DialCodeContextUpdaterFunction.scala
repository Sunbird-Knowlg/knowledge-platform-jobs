package org.sunbird.job.postpublish.functions

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.postpublish.helpers.DialHelper
import org.sunbird.job.postpublish.task.PostPublishProcessorConfig
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
      val addContextDialCodes: Map[List[String],String] = edata.getOrDefault("addContextDialCodes", Map.empty[List[String],String]).asInstanceOf[Map[List[String],String]]
      val removeContextDialCodes: Map[List[String],String] = edata.getOrDefault("removeContextDialCodes", Map.empty[List[String],String]).asInstanceOf[Map[List[String],String]]

      if(addContextDialCodes.nonEmpty) {
          addContextDialCodes.foreach(rec => {
            dialcodeContextUpdaterEvent(rec._1, rec._2, context)(metrics, config)
          })
      }

      if(removeContextDialCodes.nonEmpty) {
        removeContextDialCodes.foreach(rec => {
          dialcodeContextUpdaterEvent(rec._1, rec._2, context)(metrics, config)
        })
      }

      metrics.incCounter(config.dialcodeContextUpdaterSuccessCount)
    } catch {
      case ex: Throwable =>
        logger.error(s"Error while processing message for DIAL Code Context Updater  : ${edata}.", ex)
        metrics.incCounter(config.dialcodeContextUpdaterFailedCount)
        throw ex
    }
  }



  override def metricsList(): List[String] = {
    List(config.dialcodeContextUpdaterCount, config.dialcodeContextUpdaterSuccessCount, config.dialcodeContextUpdaterFailedCount)
  }
}
