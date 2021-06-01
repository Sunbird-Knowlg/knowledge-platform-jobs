package org.sunbird.job.certgen.domain

import java.util

import org.sunbird.job.domain.reader.JobRequest

import scala.collection.JavaConverters

class Event(eventMap: java.util.Map[String, Any], partition: Int, offset: Long) extends JobRequest(eventMap, partition , offset) {

  private val jobName = "CollectionCertificateGenerator"

  import scala.collection.JavaConverters._

  def action: String = readOrDefault[String]("edata.action", "")

  def eData: Map[String, AnyRef] = readOrDefault[Map[String, AnyRef]]("edata", Map[String, AnyRef]())

  def oldId: String = readOrDefault[String]("edata.oldId", "")

  def basePath: String = readOrDefault[String]("edata.basePath", "")

  def tag: String = readOrDefault[String]("edata.tag", "")

  def templateId: String = readOrDefault[String]("edata.templateId", "")

  def svgTemplate: String = readOrDefault[String]("edata.svgTemplate", "")

  def courseName: String = readOrDefault[String]("edata.courseName", "")

  def name: String = readOrDefault[String]("edata.name", "")

  def issuedDate: String = readOrDefault[String]("edata.issuedDate", "")

  def expiryDate: String = readOrDefault[String]("edata.expiry", "")

  //def data: List[Map[String, AnyRef]] = readOrDefault[List[java.util.Map[String, AnyRef]]]("edata.data", List[java.util.Map[String, AnyRef]]()).map(d => JavaConverters.mapAsScalaMap(d).toMap)
  def data: List[Map[String, AnyRef]] = readOrDefault[List[Map[String, AnyRef]]]("edata.data", List[Map[String, AnyRef]]()).map(m => m.toMap).toList

  //def signatoryList: List[Map[String, AnyRef]] = readOrDefault[List[java.util.Map[String, AnyRef]]]("edata.signatoryList", List[java.util.Map[String, AnyRef]]()).map(d => JavaConverters.mapAsScalaMap(d).toMap)
  def signatoryList: List[Map[String, AnyRef]] = readOrDefault[List[Map[String, AnyRef]]]("edata.signatoryList", List[Map[String, AnyRef]]())

  def issuer: Map[String, AnyRef] = readOrDefault[Map[String, AnyRef]]("edata.issuer", Map[String, AnyRef]())

  def criteria: Map[String, String] = readOrDefault[Map[String, String]]("edata.criteria", Map[String, String]())

  def keys: Map[String, String] = readOrDefault[Map[String, String]]("edata.keys", Map[String, String]())

  def logo: String = readOrDefault[String]("edata.logo", "")

  def certificateDescription: String = readOrDefault[String]("edata.description", "")

  def related: Map[String, AnyRef] = readOrDefault[Map[String, AnyRef]]("edata.related", Map[String, AnyRef]())

}
