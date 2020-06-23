package org.sunbird.kp.course.functions

import java.lang.reflect.Type
import java.util

import com.google.gson.reflect.TypeToken
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.sunbird.async.core.job.{BaseProcessFunction, Metrics}
import org.sunbird.kp.course.task.CourseAggregatorConfig

class Aggregator(config: CourseAggregatorConfig)(implicit val stringTypeInfo: TypeInformation[String])
  extends BaseProcessFunction[util.Map[String, AnyRef], util.Map[String, AnyRef]](config) {

  val mapType: Type = new TypeToken[util.Map[String, AnyRef]]() {}.getType

  override def metricsList(): List[String] = {
    List(config.successEventCount, config.failedEventCount)
  }
  /**
   * Method to process the events extraction from the batch
   *
   * @param batchEvent - Batch of telemetry events
   * @param context
   */
  override def processElement(batchEvent: util.Map[String, AnyRef],
                              context: ProcessFunction[util.Map[String, AnyRef], util.Map[String, AnyRef]]#Context,
                              metrics: Metrics): Unit = {
    println("=====================**** Batch Event" + batchEvent + "===============")

  }
}

