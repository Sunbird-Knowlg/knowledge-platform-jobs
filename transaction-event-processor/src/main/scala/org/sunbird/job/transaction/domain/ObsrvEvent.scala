package org.sunbird.job.transaction.domain

import org.slf4j.LoggerFactory
import org.sunbird.job.domain.reader.JobRequest
import org.sunbird.telemetry.dto.Telemetry
import scala.collection.JavaConverters._
import java.util.UUID


class ObsrvEvent(eventMap: java.util.Map[String, Any], override val partition: Int, override val offset: Long) extends JobRequest(eventMap, partition, offset) {

  private[this] lazy val logger = LoggerFactory.getLogger(classOf[ObsrvEvent])

  val telemetry: Telemetry = null

  def eventsList: List[Map[String, Any]] = readOrDefault[List[Map[String, Any]]]("events", List())

  def dataset = readOrDefault("dataset", "sb-knowledge-master")

  def msgid = readOrDefault("mid", UUID.randomUUID().toString)

  def syncts = readOrDefault("syncts", System.currentTimeMillis())

  def updatedList = {
//    val updatedList = eventsList.foldRight(List(eventMap.asScala.toMap))(_ :: _)
    logger.info("Updated List -> " + eventsList.foldRight(List(eventMap.asScala.toMap))(_ :: _))
    Map("events" -> List(eventMap.asScala.toMap).foldRight(eventsList)(_ :: _))
  }

  def updateEvent = {
    val updatedEvent = updatedList ++ Map(
      "dataset" -> dataset,
      "mid" -> msgid,
      "syncts" -> syncts
    )
    updatedEvent
  }
}

