package org.sunbird.user.feeds

import java.util

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.sunbird.job.{BaseJobConfig, BaseProcessFunction, Metrics}

class CreateUserFeedFunction(config: BaseJobConfig)(implicit val stringTypeInfo: TypeInformation[String])
  extends BaseProcessFunction[util.Map[String, AnyRef], String](config) {

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
  }

  override def close(): Unit = {
    super.close()
  }
  
  override def processElement(eData: util.Map[String, AnyRef],
                              context: ProcessFunction[util.Map[String, AnyRef], String]#Context,
                              metrics: Metrics): Unit = {
    println("Create USerFeed eData: " + eData)
  }

  override def metricsList(): List[String] = {
    List()
  }
}
