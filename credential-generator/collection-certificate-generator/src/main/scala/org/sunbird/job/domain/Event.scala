package org.sunbird.job.domain

import java.util

import org.sunbird.job.domain.reader.JobRequest

import scala.collection.JavaConverters

class Event(eventMap: java.util.Map[String, Any]) extends JobRequest(eventMap) {

  private val jobName = "CollectionCertificateGenerator"

  import scala.collection.JavaConverters._

  def action: String = readOrDefault[String]("edata.action", "")

  def eData: Map[String, AnyRef] = readOrDefault("edata", new util.HashMap[String, AnyRef]()).asScala.toMap

  def oldId: String = readOrDefault[String]("edata.oldId", "")

  def basePath: String = readOrDefault[String]("edata.basePath", "")

  def tag: String = readOrDefault[String]("edata.tag", "")

  def templateId: String = readOrDefault[String]("edata.templateId", "")

  def svgTemplate: String = readOrDefault[String]("edata.svgTemplate", "")

  def courseName: String = readOrDefault[String]("edata.courseName", "")

  def name: String = readOrDefault[String]("edata.name", "")

  def issuedDate: String = readOrDefault[String]("edata.issuedDate", "")

  def expiryDate: String = readOrDefault[String]("edata.expiry", "")

  def data: List[Map[String, AnyRef]] = readOrDefault("edata.data", new util.ArrayList[java.util.Map[String, AnyRef]]()).asScala.toList.map(d => JavaConverters.mapAsScalaMap(d).toMap)

  def signatoryList: List[Map[String, AnyRef]] = readOrDefault("edata.signatoryList", new util.ArrayList[java.util.Map[String, AnyRef]]()).asScala.toList.map(d => JavaConverters.mapAsScalaMap(d).toMap)

  def issuer: Map[String, AnyRef] = JavaConverters.mapAsScalaMap(readOrDefault("edata.issuer", new util.HashMap[String, AnyRef]())).toMap

  def criteria: Map[String, String] = JavaConverters.mapAsScalaMap(readOrDefault("edata.criteria", new util.HashMap[String, String]())).toMap

  def keys: Map[String, String] = JavaConverters.mapAsScalaMap(readOrDefault("edata.keys", new util.HashMap[String, String]())).toMap

  def logo: String = readOrDefault[String]("edata.logo", "")

  def certificateDescription: String = readOrDefault[String]("edata.description", "")

  def related: Map[String, AnyRef] = JavaConverters.mapAsScalaMap(readOrDefault("edata.related", new util.HashMap[String, AnyRef]())).toMap

  def validEvent(): Boolean = {
    true
  }


}
