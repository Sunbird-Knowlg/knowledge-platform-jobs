package org.sunbird.kp.course.functions

import java.{lang, util}

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.sunbird.async.core.job.{Metrics, WindowBaseProcessFunction}
import org.sunbird.kp.course.task.CourseMetricsAggregatorConfig

class Router(config: CourseMetricsAggregatorConfig)(implicit val stringTypeInfo: TypeInformation[String]) extends WindowBaseProcessFunction[util.Map[String, AnyRef], String, String](config) {

  override def metricsList(): List[String] = {

    List(config.totalEventsCount, config.batchEnrolmentUpdateEventCount)
  }

  override def process(key: String, context: ProcessWindowFunction[util.Map[String, AnyRef], String, String, TimeWindow]#Context, events: lang.Iterable[util.Map[String, AnyRef]], metrics: Metrics): Unit = {

    events.forEach(event => {
      metrics.incCounter(config.totalEventsCount)
      val eventData: util.Map[String, AnyRef] = event.get("edata").asInstanceOf[util.Map[String, AnyRef]]
      eventData.get("action").asInstanceOf[String].toUpperCase() match {
        case "BATCH-ENROLMENT-UPDATE" =>
          metrics.incCounter(config.batchEnrolmentUpdateEventCount)
          context.output(config.batchEnrolmentUpdateOutputTag, event)
        case _ => println("Here is the place to implement logic for other action type of events.")
      }
    })

  }
}
