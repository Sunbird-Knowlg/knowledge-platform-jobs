package org.sunbird.notifier

import java.lang.reflect.Type
import java.util

import com.google.gson.reflect.TypeToken
import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.sunbird.job.{BaseJobConfig, BaseProcessFunction, Metrics}

class NotifierFunction(config: BaseJobConfig)(implicit val stringTypeInfo: TypeInformation[String])
  extends BaseProcessFunction[util.Map[String, AnyRef], String](config) {

  val mapType: Type = new TypeToken[java.util.Map[String, AnyRef]]() {}.getType
  lazy private val mapper: ObjectMapper = new ObjectMapper()

  override def metricsList(): List[String] = {
    List()
  }

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
  }

  override def close(): Unit = {
    super.close()
  }

  override def processElement(eData: util.Map[String, AnyRef],
                              context: ProcessFunction[util.Map[String, AnyRef], String]#Context,
                              metrics: Metrics): Unit = {
    val userId: String = eData.getOrDefault("userId", "").asInstanceOf[String]
    val notifyTemplate: java.util.Map[String, String] = eData.get("notifyTemplate").asInstanceOf[java.util.Map[String, String]]
    //get user Details
    //notify user
  }
}
