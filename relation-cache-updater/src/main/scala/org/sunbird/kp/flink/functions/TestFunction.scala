package org.sunbird.kp.flink.functions


import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.async.core.job.{BaseProcessFunction, Metrics, WindowBaseProcessFunction}
import org.sunbird.kp.flink.task.RelationCacheUpdaterConfig

class TestFunction(config: RelationCacheUpdaterConfig)(implicit val stringTypeInfo: TypeInformation[String])  extends BaseProcessFunction[java.util.Map[String, AnyRef], String](config) {

  private[this] val logger = LoggerFactory.getLogger(classOf[TestFunction])

  override def metricsList(): List[String] = {
    List(config.successEventCount, config.failedEventCount, config.totalEventsCount)
  }

  override def processElement(event: java.util.Map[String, AnyRef], context: ProcessFunction[java.util.Map[String, AnyRef], String]#Context, metrics: Metrics): Unit = {
    metrics.incCounter(config.successEventCount)
    metrics.incCounter(config.totalEventsCount)
    println("Got event:" + event)
    logger.info("Got event logger: " + event)
  }
}
