package org.sunbird.job.functions

import java.util

import com.datastax.driver.core.{Row, TypeTokens}
import org.slf4j.LoggerFactory
import org.sunbird.job.domain.{CertTemplate, GenerateRequest}
import org.sunbird.job.task.CertificatePreProcessorConfig

import scala.collection.JavaConverters._

object IssueCertificateUtil {

  private[this] val logger = LoggerFactory.getLogger(IssueCertificateUtil.getClass)

  def getActiveUserIds(rows: util.List[Row], edata: util.Map[String, AnyRef], templateName: String, config: CertificatePreProcessorConfig): List[String] = {
    println("getActiveUserIds called : ")
    val userIds = rows.asScala.filter(row => {
      val issuedCertificates: util.List[util.Map[String, String]] = row.getList("issued_certificates", TypeTokens.mapOf(classOf[String], classOf[String]))
      val isCertIssued = !issuedCertificates.isEmpty && issuedCertificates.asScala.exists(a => a.get(config.name) == templateName)
      println("getActiveUserIds : isCertIssued : " + isCertIssued)
      row.getBool("active") && (!isCertIssued || edata.containsKey(config.reIssue) && edata.get(config.reIssue).asInstanceOf[Boolean])
    }).map(row => row.getString("userid")).toList
    userIds
  }

  def getAssessedUserIds(rows: util.List[Row], assessmentCriteria: util.Map[String, AnyRef], edata: util.Map[String, AnyRef], config: CertificatePreProcessorConfig): List[String] = {
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
  private def getAssessedUser(rows: util.List[Row]): Map[String, Double] = {
    var userScore = scala.collection.mutable.Map[String, Map[String, Double]]()
    rows.asScala.map(row => {
      val userId = row.getString("user_id")
      if (!userScore.contains(userId)) {
        val scoreMap = userScore.get(userId).asInstanceOf[Map[String, Double]]
        userScore += (userId -> Map("score" -> (scoreMap.get("score").asInstanceOf[Double] + row.getDouble("score")),
          ("maxScore" -> (scoreMap.get("maxScore").asInstanceOf[Double] + row.getDouble("total_max_score")))))
      } else {
        userScore += (userId -> Map("score" -> row.getDouble("score"), "maxScore" -> row.getDouble("total_max_score")))
      }
    })
    println("getAssessedUserIds userScore : " + userScore)
    val assessedUserMap: Map[String, Double] = userScore.map(score => {
      Map(score._1 -> ((score._2.get("score").asInstanceOf[Double] * 100) / score._2.get("maxScore").asInstanceOf[Double]))
    }).flatten.toMap
    println("getAssessedUserIds assessedUserMap : " + assessedUserMap)
    assessedUserMap
  }

  private def getAssessmentOperation(assessmentCriteria: util.Map[String, AnyRef]): Map[String, AnyRef] = {
    if (assessmentCriteria.get("score").isInstanceOf[util.Map[String, Double]]) {
      assessmentCriteria.get("score").asInstanceOf[Map[String, AnyRef]]
    } else {
      Map("EQ" -> assessmentCriteria.get("score"))
    }
  }

  def prepareTemplate(template: util.Map[String, AnyRef], config: CertificatePreProcessorConfig): CertTemplate = {
    CertTemplate(templateId = template.getOrDefault(config.identifier, "").asInstanceOf[String],
      name = template.getOrDefault(config.name, "").asInstanceOf[String],
      notifyTemplate = template.getOrDefault(config.notifyTemplate, new util.HashMap()).asInstanceOf[util.Map[String, AnyRef]],
      signatoryList = template.getOrDefault(config.signatoryList, new util.HashMap()).asInstanceOf[util.ArrayList[util.Map[String, String]]],
      issuer = template.getOrDefault(config.issuer, new util.HashMap()).asInstanceOf[util.Map[String, String]],
      criteria = template.getOrDefault(config.criteria, new util.HashMap()).asInstanceOf[util.Map[String, String]])
  }

  def prepareGenerateRequest(edata: util.Map[String, AnyRef], template: CertTemplate, userId: String, config: CertificatePreProcessorConfig): GenerateRequest = {
    GenerateRequest(batchId = edata.get(config.batchId).asInstanceOf[String],
      courseId = edata.get(config.courseId).asInstanceOf[String],
      userId = userId,
      template = template,
      reIssue = if (edata.containsKey(config.reIssue)) edata.get(config.reIssue).asInstanceOf[Boolean] else false)
  }
}
