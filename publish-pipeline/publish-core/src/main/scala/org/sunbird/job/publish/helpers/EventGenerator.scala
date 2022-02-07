package org.sunbird.job.publish.helpers

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.commons.lang3.StringUtils
import org.sunbird.job.exception.EventException
import org.sunbird.job.publish.core.{PublishChainEvent, PublishCoreMetadata}
import org.sunbird.job.util.{JSONUtil, OntologyEngineContext}

import java.util
import java.util.UUID
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

object EventGenerator {
  private val mapper = new ObjectMapper
  private val beJobRequesteventId = "BE_JOB_REQUEST";
  private val iteration = "1"

  @throws[Exception]
  def pushPublishEvent(publishChainEvent: Map[String, AnyRef],  publishMetadata: PublishCoreMetadata, inputTopic: String, actorId: String)(implicit oec: OntologyEngineContext): Unit = {
    val publishChainEventMap: scala.collection.mutable.Map[String, AnyRef] = collection.mutable.Map(publishChainEvent.toSeq: _*)
    val (actor, context, objData, eData) = generatePublishEventMetadata(publishChainEventMap, publishMetadata, actorId)
    val beJobRequestEvent: String = logInstructionEvent(actor, context, objData, eData)
    if (StringUtils.isBlank(beJobRequestEvent)) throw new EventException("Generated event is empty.")
    try {
      oec.kafkaClient.send(beJobRequestEvent, inputTopic)
    } catch {
      case e: Throwable => {
        throw new Exception(e.getMessage)

      }
    }
  }

  @throws[Exception]
  def pushPublishChainEvent(publishChainEvent: scala.collection.mutable.Map[String, AnyRef], publishChainList: List[scala.collection.mutable.Map[String, AnyRef]], inputTopic: String)(implicit oec: OntologyEngineContext): Unit = {
    val (actor, context, objData, eData) = generatePublishChainEventMetadata(publishChainEvent, publishChainList)
    val beJobRequestEvent: String = logInstructionEvent(actor, context, objData, eData)
    if (StringUtils.isBlank(beJobRequestEvent)) throw new EventException("Generated event is empty.")
    try {
      oec.kafkaClient.send(beJobRequestEvent, inputTopic)
    } catch {
      case e: Throwable => {
        throw new Exception(e.getMessage)
      }
    }
  }

  def generatePublishEventMetadata(publishChainEvent: scala.collection.mutable.Map[String, AnyRef], publishMetadata: PublishCoreMetadata, actorId: String): (Map[String, AnyRef], Map[String, AnyRef], Map[String, AnyRef], util.Map[String, AnyRef]) = {
    val metadata: util.Map[String, AnyRef] = publishChainEvent
    metadata.put("publishChainMetadata", JSONUtil.serialize(publishMetadata))
    val actor = Map("id" -> actorId, "type" -> "System".asInstanceOf[AnyRef])
    val context = Map("channel" -> publishMetadata.context.getOrElse("channel", ""), "pdata" -> Map("id" -> "org.sunbird.platform", "ver" -> "1.0").asJava, "env" -> publishMetadata.context.getOrElse("channel", ""))
    val objData = Map("id" -> publishChainEvent.getOrElse("identifier", ""), "ver" -> publishMetadata.obj.getOrElse("ver", ""))
    val eData: util.Map[String, AnyRef] = new util.HashMap[String, AnyRef] {
      {
        put("action", "publish")
        put("publish_type", publishMetadata.eData.getOrElse("publishType", "public"))
        put("metadata", metadata)
      }
    }
    (actor, context, objData, eData)
  }

  def generatePublishChainEventMetadata(publishChainEvent: scala.collection.mutable.Map[String, AnyRef], publishChainList: List[scala.collection.mutable.Map[String, AnyRef]]): (Map[String, AnyRef], Map[String, AnyRef], Map[String, AnyRef], util.Map[String, AnyRef]) = {
    val actor: Map[String, AnyRef] = publishChainEvent.getOrElse("actor", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
    val context: Map[String, AnyRef] = publishChainEvent.getOrElse("context", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
    val objData: Map[String, AnyRef] = publishChainEvent.getOrElse("object", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]

    val eData: util.Map[String, AnyRef] = new util.HashMap[String, AnyRef] {
      {
        put("action", "publishchain")
        put("publish_type", "public")
        put("publishchain", publishChainList.asJava)
      }
    }
    (actor, context, objData, eData)
  }


  def logInstructionEvent(actor: util.Map[String, AnyRef], context: util.Map[String, AnyRef], `object`: util.Map[String, AnyRef], edata: util.Map[String, AnyRef]): String = {
    var jsonMessage: String = null
    try {
      val unixTime = System.currentTimeMillis
      val mid = "LP." + System.currentTimeMillis + "." + UUID.randomUUID
      edata.put("iteration", iteration)
      val te = new PublishChainEvent(beJobRequesteventId, unixTime, mid, actor, context, `object`, edata)
      mapper.registerModule(new DefaultScalaModule)
      jsonMessage = mapper.writeValueAsString(te)
    } catch {
      case e: Exception =>
        throw new Exception(e.getMessage)
    }
    jsonMessage
  }

}
