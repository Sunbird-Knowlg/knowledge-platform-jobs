package org.sunbird.job.functions

import java.util

import com.datastax.driver.core.{Row, TypeTokens}
import org.apache.commons.collections.CollectionUtils
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.sunbird.job.task.CertificatePreProcessorConfig

import scala.collection.JavaConverters._

object IssueCertificateUtil {

  private[this] val logger = LoggerFactory.getLogger(IssueCertificateUtil.getClass)

  def getActiveUserIds(rows: util.List[Row], edata: util.Map[String, AnyRef], templateName: String, config: CertificatePreProcessorConfig): List[String] = {
    println("getActiveUserIds called : ")
    val userIds = rows.asScala.filter(row => {
      val issuedCertificates = row.getList("issued_certificates", TypeTokens.mapOf(classOf[String], classOf[String]))
      val isCertIssued = (CollectionUtils.isNotEmpty(issuedCertificates)) &&
        (CollectionUtils.isNotEmpty(issuedCertificates.asScala.toStream.filter(cert => {
          StringUtils.equalsIgnoreCase(templateName, cert.getOrDefault("name", ""))
        }).toList.asJava))
      row.getBool("active") && (!isCertIssued || edata.containsKey(config.reIssue) && edata.get(config.reIssue).asInstanceOf[Boolean])
    }).map(row => row.getString("userid")).toList
    userIds
  }

  def getAssessedUserIds(rows: util.List[Row], assessmentCriteria: util.Map[String, AnyRef], edata: util.Map[String, AnyRef], config: CertificatePreProcessorConfig): List[String] = {
    println("getAssessedUserIds called : ")
    val userScores:Map[String, Double] = getAssessedUser(rows)
    val criteria:Map[String, AnyRef] = getAssessmentOperation(assessmentCriteria)
    if (userScores.nonEmpty) {
      userScores.map(user => {
        if (EventValidator.isValidAssessUser(user._2, criteria))
          user._1
      }).toMap.toList
    } else {
      logger.info("No assessment score for batchID: " + edata.get(config.batchId) + " and courseId: " + edata.get(config.courseId))
      List()
    }
  }

  private def getAssessedUser(rows: util.List[Row]): Map[String, Double] = {
    var userScore = scala.collection.mutable.Map[String, Map[String, Double]]()
    rows.asScala.map(row => {
      val userId = row.getString("user_id")
      if (!userScore.contains(userId)) {
        var scoreMap = userScore.get(userId).asInstanceOf[Map[String, Double]]
        scoreMap += ("score" -> (scoreMap.get("score").asInstanceOf[Double] + row.getDouble("score")))
        scoreMap += ("maxScore" -> (scoreMap.get("maxScore").asInstanceOf[Double] + row.getDouble("total_max_score")))
      } else {
        userScore += (userId -> Map("score" -> row.getDouble("score"), "maxScore" -> row.getDouble("total_max_score")))
      }
    })
    println("getAssessedUserIds userScore : " + userScore)
    val assessedUserMap = userScore.toStream.map(score => {
      Map(score._1 -> ((score._2.get("score").asInstanceOf[Double] * 100) / score._2.get("maxScore").asInstanceOf[Double]))
    }).toMap
    println("getAssessedUserIds assessedUserMap : " + assessedUserMap)
    assessedUserMap.toMap
  }

  private def getAssessmentOperation(assessmentCriteria: util.Map[String, AnyRef]): Map[String, AnyRef] = {
    if (assessmentCriteria.get("score").isInstanceOf[util.Map]) {
      assessmentCriteria.asScala.get("score").asInstanceOf[Map[String, AnyRef]]
    } else {
      Map("EQ" -> assessmentCriteria.get("score"))
    }
  }

}
