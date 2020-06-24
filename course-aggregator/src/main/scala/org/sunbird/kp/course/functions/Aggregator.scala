package org.sunbird.kp.course.functions

import java.lang.reflect.Type
import java.util

import com.google.gson.reflect.TypeToken
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.sunbird.async.core.job.{BaseProcessFunction, Metrics}
import org.sunbird.async.core.util.CassandraUtil
import org.sunbird.kp.course.task.CourseAggregatorConfig

class Aggregator(config: CourseAggregatorConfig)(implicit val stringTypeInfo: TypeInformation[String],
                                                 @transient var cassandraUtil: CassandraUtil = null
)
  extends BaseProcessFunction[util.Map[String, AnyRef], util.Map[String, AnyRef]](config) {

  val mapType: Type = new TypeToken[util.Map[String, AnyRef]]() {}.getType

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    cassandraUtil = new CassandraUtil(config.dbHost, config.dbPort)
  }

  override def close(): Unit = {
    super.close()
  }

  /**
   * List of metrics capturing
   * @return
   */
  override def metricsList(): List[String] = {
    List(config.successEventCount, config.failedEventCount, config.totalEventsCount, config.dbUpdateCount)
  }

  /**
   * Method to process course progress events
   *
   * @param event -
   * @param context
   */
  override def processElement(event: util.Map[String, AnyRef],
                              context: ProcessFunction[util.Map[String, AnyRef],
                                util.Map[String, AnyRef]]#Context,
                              metrics: Metrics): Unit = {
    metrics.incCounter(config.totalEventsCount)
    println("=====Event===" + event)
  }
}

