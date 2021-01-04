package org.sunbird.job.domain

import java.util

import org.apache.commons.lang3.StringUtils
import org.sunbird.job.domain.reader.JobRequest

class Event(eventMap: java.util.Map[String, Any]) extends JobRequest(eventMap) {

  private val jobName = "CollectionCertificateGenerator"

  import scala.collection.JavaConverters._

  def action: String = readOrDefault[String]("edata.action", "")

  def eData: Map[String, AnyRef] = readOrDefault("edata", new util.HashMap[String, AnyRef]()).asScala.toMap

  def oldId: String = readOrDefault[String]("edata.oldId", "")

  def basePath: String = readOrDefault[String]("edata.basePath", "")

  def tag: String = readOrDefault[String]("edata.tag", "")

  def templateId: String = readOrDefault[String]("edata.templateId", "")

  def svgTemplate: String = read[String]("edata.svgTemplate").get

  def courseName: String = read[String]("edata.courseName").get

  def name: String = read[String]("edata.name").get

  def issuedDate: String = read[String]("edata.issuedDate").get

  def data: List[Map[String, AnyRef]] = readOrDefault("edata.data", new util.ArrayList[Map[String, AnyRef]]()).asScala.toList

  def signatoryList: List[Map[String, AnyRef]] = readOrDefault("edata.signatoryList", new util.ArrayList[Map[String, AnyRef]]()).asScala.toList

  def issuer: Map[String, AnyRef] = readOrDefault("edata.issuer", new util.HashMap[String, AnyRef]()).asScala.toMap

  def criteria: Map[String, AnyRef] = readOrDefault("edata.criteria", new util.HashMap[String, AnyRef]()).asScala.toMap

  def keys: Map[String, String] = readOrDefault("edata.keys", new util.HashMap[String, String]()).asScala.toMap

  def logo: String = read[String]("edata.logo").get

  def certificateDescription: String = read[String]("edata.description").get

  def related: Map[String, AnyRef] = readOrDefault("edata.related", new util.HashMap[String, AnyRef]()).asScala.toMap


  def validEvent(): Boolean = {
    true
  }


}
