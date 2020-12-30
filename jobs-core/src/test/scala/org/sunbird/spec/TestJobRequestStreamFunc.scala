package org.sunbird.spec

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.sunbird.job.{BaseProcessFunction, Metrics}
import org.sunbird.job.domain.reader.JobRequest

class TestJobRequestStreamFunc(config: BaseProcessTestConfig) extends BaseProcessFunction[TestJobRequest, TestJobRequest](config) {

  override def metricsList(): List[String] = {
    val metrics= List(config.jobRequestEventCount)
    metrics
  }

  override def processElement(event: TestJobRequest, context: ProcessFunction[TestJobRequest, TestJobRequest]#Context, metrics: Metrics): Unit = {
    metrics.get(config.jobRequestEventCount)
    metrics.reset(config.jobRequestEventCount)
    metrics.incCounter(config.jobRequestEventCount)
    context.output(config.jobRequestOutputTag, event)
  }
}

class TestJobRequest(map: java.util.Map[String, Any]) extends JobRequest(map) {

}