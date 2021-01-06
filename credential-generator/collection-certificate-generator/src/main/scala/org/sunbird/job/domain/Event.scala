package org.sunbird.job.domain

import java.util

import org.sunbird.job.domain.reader.JobRequest

import scala.collection.JavaConverters

class Event(eventMap: java.util.Map[String, Any]) extends JobRequest(eventMap) {

  private val jobName = "CollectionCertificateGenerator"

  import scala.collection.JavaConverters._

  def action: String = readOrDefault[String]("edata.action", "")

  def eData: Map[String, AnyRef] = JavaConverters.mapAsScalaMap(readOrDefault("edata", new util.HashMap[String, AnyRef]())).toMap

  def oldId: String = readOrDefault[String]("edata.oldId", "")

  def basePath: String = readOrDefault[String]("edata.basePath", "")

  def tag: String = readOrDefault[String]("edata.tag", "")

  def templateId: String = readOrDefault[String]("edata.templateId", "")

  def svgTemplate: String = read[String]("edata.svgTemplate").get

  def courseName: String = read[String]("edata.courseName").get

  def name: String = read[String]("edata.name").get

  def issuedDate: String = read[String]("edata.issuedDate").get

  def data: List[Map[String, AnyRef]] = readOrDefault("edata.data", new util.ArrayList[java.util.Map[String, AnyRef]]()).asScala.toList.map(d => JavaConverters.mapAsScalaMap(d).toMap)

  def signatoryList: List[Map[String, AnyRef]] = readOrDefault("edata.signatoryList", new util.ArrayList[java.util.Map[String, AnyRef]]()).asScala.toList.map(d => JavaConverters.mapAsScalaMap(d).toMap)

  def issuer: Map[String, AnyRef] = JavaConverters.mapAsScalaMap(readOrDefault("edata.issuer", new util.HashMap[String, AnyRef]())).toMap

  def criteria: Map[String, AnyRef] = JavaConverters.mapAsScalaMap(readOrDefault("edata.criteria", new util.HashMap[String, AnyRef]())).toMap

  def keys: Map[String, String] = JavaConverters.mapAsScalaMap(readOrDefault("edata.keys", new util.HashMap[String, String]())).toMap

  def logo: String = read[String]("edata.logo").get

  def certificateDescription: String = read[String]("edata.description").get

  def related: Map[String, AnyRef] = JavaConverters.mapAsScalaMap(readOrDefault("edata.related", new util.HashMap[String, AnyRef]())).toMap


  def validEvent(): Boolean = {
    true
  }


}
