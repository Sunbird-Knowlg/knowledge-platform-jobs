package org.sunbird.spec

import java.util
import scala.collection.mutable.Map
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.sunbird.job.{BaseProcessFunction, Metrics}


class TestMapStreamFunc(config: BaseProcessTestConfig)(implicit val stringTypeInfo: TypeInformation[String])
  extends BaseProcessFunction[Map[String, AnyRef], Map[String, AnyRef]](config) {

  override def metricsList(): List[String] = {
    List(config.mapEventCount)
  }

  override def processElement(event: Map[String, AnyRef],
                              context: ProcessFunction[Map[String, AnyRef], Map[String, AnyRef]]#Context,
                              metrics: Metrics): Unit = {
        metrics.get(config.mapEventCount)
        metrics.reset(config.mapEventCount)
        metrics.incCounter(config.mapEventCount)
        context.output(config.mapOutputTag, event)
  }

}
