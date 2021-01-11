package org.sunbird.incredible

import java.io.File

import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.core.JsonGenerator.Feature
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.module.scala.DefaultScalaModule

object JsonUtils {


  @transient val mapper = new ObjectMapper()
  mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
  mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)
  mapper.configure(Feature.WRITE_BIGDECIMAL_AS_PLAIN, true)
  mapper.setSerializationInclusion(Include.NON_NULL)
  mapper.setSerializationInclusion(Include.NON_ABSENT)
  mapper.registerModule(DefaultScalaModule)


  @throws(classOf[Exception])
  def writeToJsonFile(file: File, obj: AnyRef) = {
    mapper.writeValue(file, obj)
  }



  @throws(classOf[Exception])
  def serialize(obj: AnyRef): String = {
    mapper.writeValueAsString(obj)
  }


}
