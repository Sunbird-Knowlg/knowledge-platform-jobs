package org.sunbird.job.functions

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.sunbird.job.{BaseProcessFunction, Metrics}
import org.sunbird.job.domain.EnrolmentComplete
import org.sunbird.job.task.ActivityAggregateUpdaterConfig
import org.sunbird.job.util.CassandraUtil

class EnrolmentCompleteFunction(config: ActivityAggregateUpdaterConfig)(implicit val stringTypeInfo: TypeInformation[EnrolmentComplete], @transient var cassandraUtil: CassandraUtil = null)
  extends BaseProcessFunction[EnrolmentComplete, String](config) {
  override def processElement(event: EnrolmentComplete, context: ProcessFunction[EnrolmentComplete, String]#Context, metrics: Metrics): Unit = {
    println("Enrolment complete action:" + event)
  }

  override def metricsList(): List[String] = {
    List()
  }
}
