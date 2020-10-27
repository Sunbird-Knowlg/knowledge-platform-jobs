package org.sunbird.job.functions

import java.util

import com.datastax.driver.core.{Row, TypeTokens}
import com.google.gson.Gson
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.`type`.TypeReference
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper
import org.slf4j.LoggerFactory
import org.sunbird.job.domain.{CertTemplate, GenerateRequest}
import org.sunbird.job.task.CertificatePreProcessorConfig

import scala.collection.JavaConverters._

object IssueCertificateUtil {

  private[this] val logger = LoggerFactory.getLogger(IssueCertificateUtil.getClass)
  lazy private val gson = new Gson()
  lazy private val mapper: ObjectMapper = new ObjectMapper()

  def getActiveUserIds(rows: util.List[Row], edata: util.Map[String, AnyRef], templateName: String)
                      (implicit config: CertificatePreProcessorConfig): List[String] = {
    println("getActiveUserIds called : ")
    val userIds = rows.asScala.filter(row => {
      val issuedCertificates: util.List[util.Map[String, String]] = row.getList(config.issued_certificates, TypeTokens.mapOf(classOf[String], classOf[String]))
      val isCertIssued = !issuedCertificates.isEmpty && issuedCertificates.asScala.exists(a => a.get(config.name) == templateName)
      println("getActiveUserIds : isCertIssued : " + isCertIssued)
      row.getBool(config.active) && (!isCertIssued || edata.containsKey(config.reIssue) && edata.get(config.reIssue).asInstanceOf[Boolean])
    }).map(row => row.getString("userid")).toList
    userIds
  }

  def getAssessedUserIds(rows: util.List[Row], assessmentCriteria: util.Map[String, AnyRef], edata: util.Map[String, AnyRef])
                        (implicit config: CertificatePreProcessorConfig): List[String] = {
    println("getAssessedUserIds called : ")
    val userScores: Map[String, Double] = getAssessedUser(rows)
    println("getAssessedUserIds userScores : " + userScores)
    // need to check getAssessmentOperation
    val criteria: Map[String, AnyRef] = getAssessmentOperation(assessmentCriteria)
    if (userScores.nonEmpty) {
      userScores.filter(user => EventValidator.isValidAssessUser(user._2, criteria)).keys.toList
    } else {
      logger.info("No assessment score for batchID: " + edata.get(config.batchId) + " and courseId: " + edata.get(config.courseId))
      List()
    }
  }

  // see if can change in better way
  private def getAssessedUser(rows: util.List[Row])
                             (implicit config: CertificatePreProcessorConfig): Map[String, Double] = {
    var userScore = scala.collection.mutable.Map[String, Map[String, Double]]()
    rows.asScala.map(row => {
      val userId = row.getString("user_id")
      if (userScore.contains(userId)) {
        val scoreMap = userScore.get(userId).asInstanceOf[Map[String, Double]]
        userScore += (userId -> Map(config.score -> (scoreMap.get(config.score).asInstanceOf[Double] + row.getDouble(config.score)),
          (config.maxScore -> (scoreMap.get(config.maxScore).asInstanceOf[Double] + row.getDouble(config.total_max_score)))))
      } else {
        userScore += (userId -> Map(config.score -> row.getDouble(config.score), config.maxScore -> row.getDouble(config.total_max_score)))
      }
    })
    println("getAssessedUserIds userScore : " + userScore)
    val assessedUserMap: Map[String, Double] = userScore.map(score => {
      Map(score._1 -> ((score._2.asJava.get(config.score) * 100) / score._2.asJava.get(config.maxScore)))
    }).flatten.toMap
    println("getAssessedUserIds assessedUserMap : " + assessedUserMap)
    assessedUserMap
  }

  private def getAssessmentOperation(assessmentCriteria: util.Map[String, AnyRef])
                                    (implicit config: CertificatePreProcessorConfig): Map[String, AnyRef] = {
    if (assessmentCriteria.get(config.score).isInstanceOf[util.HashMap[String, Double]]) {
      assessmentCriteria.get(config.score).asInstanceOf[Map[String, AnyRef]]
    } else {
      Map("EQ" -> assessmentCriteria.get(config.score))
    }
  }

  def prepareTemplate(template: util.Map[String, AnyRef])
                     (implicit config: CertificatePreProcessorConfig): CertTemplate = {
    CertTemplate(templateId = template.getOrDefault(config.identifier, "").asInstanceOf[String],
      name = template.getOrDefault(config.name, "").asInstanceOf[String],
      signatoryList = mapper.readValue(template.getOrDefault(config.signatoryList, "").asInstanceOf[String], new TypeReference[util.ArrayList[util.Map[String,String]]]() {}),
      issuer = mapper.readValue(template.getOrDefault(config.issuer, "").asInstanceOf[String], new TypeReference[util.Map[String,AnyRef]]() {}),
      criteria = mapper.readValue(template.getOrDefault(config.criteria, "").asInstanceOf[String], new TypeReference[util.Map[String,AnyRef]]() {}),
      svgTemplate = template.getOrDefault(config.svgTemplate, "").asInstanceOf[String])
  }

  def prepareGenerateRequest(edata: util.Map[String, AnyRef], certTemplate: CertTemplate, userId: String)
                            (implicit config: CertificatePreProcessorConfig): GenerateRequest = {
    val template = gson.fromJson(gson.toJson(certTemplate), new util.LinkedHashMap[String, AnyRef]().getClass).asInstanceOf[util.Map[String, AnyRef]]
    GenerateRequest(batchId = edata.get(config.batchId).asInstanceOf[String],
      courseId = edata.get(config.courseId).asInstanceOf[String],
      userId = userId,
      template = template,
      reIssue = if (edata.containsKey(config.reIssue)) edata.get(config.reIssue).asInstanceOf[Boolean] else false)
  }
}
