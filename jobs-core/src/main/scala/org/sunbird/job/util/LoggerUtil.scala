package org.sunbird.job.util

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.databind.node.ObjectNode

object LoggerUtil {
  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

  def getErrorLogs(errCode: String, errType: String, requestId: String, stacktrace: String): String = {
    val rootNode: ObjectNode = mapper.createObjectNode()
    val edataNode: ObjectNode = mapper.createObjectNode()
    edataNode.put("err", errCode)
    edataNode.put("errtype", errType)
    edataNode.put("requestid", requestId)
    edataNode.put("stacktrace", stacktrace)
    rootNode.put("eid", "ERROR")
    rootNode.set("edata", edataNode)
    mapper.writeValueAsString(rootNode)
  }

  def getEntryLogs(jobName: String, requestId: String, message: String, params: Map[String, String] = Map()): String = {
    val rootNode: ObjectNode = mapper.createObjectNode()
    val edataNode: ObjectNode = mapper.createObjectNode()
    val paramsNode: ObjectNode = mapper.createObjectNode()
    params.foreach { case (key, value) =>
      paramsNode.put(key, value)
    }
    edataNode.put("type", "system")
    edataNode.put("level", "TRACE")
    edataNode.put("requestid", requestId)
    val msg: String = s"""ENTRY:${jobName} - ${message}"""
    edataNode.put("message", msg)
    edataNode.set("params", paramsNode)
    rootNode.put("eid", "LOG")
    rootNode.set("edata", edataNode)
    mapper.writeValueAsString(rootNode)
  }

  def getExitLogs(jobName: String, requestId: String, message: String, params: Map[String, String] = Map()): String = {
    val rootNode: ObjectNode = mapper.createObjectNode()
    val edataNode: ObjectNode = mapper.createObjectNode()
    val paramsNode: ObjectNode = mapper.createObjectNode()
    params.foreach { case (key, value) =>
      paramsNode.put(key, value)
    }
    edataNode.put("type", "system")
    edataNode.put("level", "TRACE")
    edataNode.put("requestid", requestId)
    val msg: String = s"""EXIT:${jobName} - ${message}"""
    edataNode.put("message", msg)
    edataNode.set("params", paramsNode)
    rootNode.put("eid", "LOG")
    rootNode.set("edata", edataNode)
    mapper.writeValueAsString(rootNode)
  }
}