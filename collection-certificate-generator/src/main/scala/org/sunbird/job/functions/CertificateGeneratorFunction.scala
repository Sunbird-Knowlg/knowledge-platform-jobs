package org.sunbird.job.functions

import java.lang.reflect.Type
import java.text.SimpleDateFormat
import java.util

import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.{BaseProcessFunction, Metrics}
import org.sunbird.job.task.CertificateGeneratorConfig

class CertificateGeneratorFunction(config: CertificateGeneratorConfig)
                                  (implicit val stringTypeInfo: TypeInformation[String])
  extends BaseProcessFunction[java.util.Map[String, AnyRef], String](config) {


  private[this] val logger = LoggerFactory.getLogger(classOf[CertificateGeneratorFunction])
  val mapType: Type = new TypeToken[java.util.Map[String, AnyRef]]() {}.getType
  //  val storageType: String = config.storageType
  val directory: String = "certificates/"
  lazy private val mapper: ObjectMapper = new ObjectMapper()
  mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL)
  lazy private val gson = new Gson()
  val formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ")

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
  }

  override def close(): Unit = {
    super.close()
  }
  
  override def processElement(event: util.Map[String, AnyRef],
                              context: ProcessFunction[util.Map[String, AnyRef], String]#Context,
                              metrics: Metrics): Unit = {
    println("Certificate data: " + event)
  }

  override def metricsList(): List[String] = {
    List()
  }
}
