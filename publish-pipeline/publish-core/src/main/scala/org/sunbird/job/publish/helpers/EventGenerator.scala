package org.sunbird.job.publish.helpers

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.commons.lang3.StringUtils
import org.sunbird.job.exception.EventException
import org.sunbird.job.publish.core.Event
import org.sunbird.job.util.OntologyEngineContext

import java.util
import java.util.UUID
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

object EventGenerator {
  private val mapper = new ObjectMapper
  private val beJobRequestEventId = "BE_JOB_REQUEST";
  private val iteration = "1"

  @throws[Exception]
  def pushPublishEvent(publishChainEvent: Map[String, AnyRef],  event: collection.mutable.Map[String, Any], inputTopic: String, actorId: String)(implicit oec: OntologyEngineContext): Unit = {
    val publishChainEventMap: scala.collection.mutable.Map[String, AnyRef] = collection.mutable.Map(publishChainEvent.toSeq: _*)
    val (actor, context, objData, eData) = generatePublishEventMetadata(publishChainEventMap, event, actorId)
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

  def generatePublishEventMetadata(publishChainEvent: scala.collection.mutable.Map[String, AnyRef],event: collection.mutable.Map[String, Any], actorId: String): (Map[String, Any], Map[String, Any], Map[String, Any], util.Map[String, AnyRef]) = {
    val metadata: util.Map[String, AnyRef] = publishChainEvent
    mapper.registerModule(new DefaultScalaModule)
    val publishChainMetadata : String =  mapper.writeValueAsString(event)
    metadata.put("publishChainMetadata", publishChainMetadata)
    val actor = Map("id" -> actorId, "type" -> "System".asInstanceOf[Any])
    val publishData : Map[String, Any]  = event.getOrElse("edata",null).asInstanceOf[Map[String, AnyRef]]
    val context : Map[String, Any] = event.getOrElse("context",null).asInstanceOf[Map[String, AnyRef]]
    val objData :Map[String, Any] = event.getOrElse("object",null).asInstanceOf[Map[String, AnyRef]]
    val eData: util.Map[String, AnyRef] = new util.HashMap[String, AnyRef] {
      {
        put("action", "publish")
        put("publish_type", publishData.getOrElse("publish_type","").toString)
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


  def logInstructionEvent(actor: util.Map[String, Any], context: util.Map[String, Any], `object`: util.Map[String, Any], edata: util.Map[String, AnyRef]): String = {
    var jsonMessage: String = null
    try {
      val unixTime = System.currentTimeMillis
      val mid = "LP." + System.currentTimeMillis + "." + UUID.randomUUID
      edata.put("iteration", iteration)
      val te = Event(beJobRequestEventId, unixTime, mid, actor, context, `object`, edata)
      mapper.registerModule(new DefaultScalaModule)
      jsonMessage = mapper.writeValueAsString(te)

    } catch {
      case e: Exception =>
        throw new Exception(e.getMessage)
    }
    jsonMessage
  }

}
