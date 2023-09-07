package org.sunbird.job.transaction.domain

import org.sunbird.job.domain.reader.JobRequest

import java.util
import java.util.UUID


class EventCache (eventMap: java.util.Map[String, Any], partition: Int, offset: Long) extends JobRequest(eventMap, partition, offset) {

  private val jobName = "ObrvMetadataGenerator"

  def events = readOrDefault[java.util.List[Event]]("events", new util.ArrayList[Event]()).asInstanceOf[List[Event]]

  def dataset: String = readOrDefault("dataset","sb-knowledge-master")

  def msgid: UUID = readOrDefault("mid",UUID.randomUUID())

  def syncts: Long = readOrDefault("syncts", System.currentTimeMillis())
}
