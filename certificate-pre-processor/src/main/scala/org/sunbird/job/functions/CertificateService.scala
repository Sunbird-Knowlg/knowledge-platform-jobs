package org.sunbird.job.functions

import java.util
import java.util.stream.Collectors

import com.datastax.driver.core.TypeTokens
import com.datastax.driver.core.querybuilder.QueryBuilder
import org.apache.commons.collections.{CollectionUtils, MapUtils}
import org.apache.commons.lang3.StringUtils
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper
import org.slf4j.LoggerFactory
import org.sunbird.job.Metrics
import org.sunbird.job.cache.DataCache
import org.sunbird.job.task.{CertificatePreProcessorConfig, CertificatePreProcessorStreamTask}
import org.sunbird.job.util.CassandraUtil

import scala.collection.JavaConverters._


class CertificateService(config: CertificatePreProcessorConfig)
                        (implicit val metrics: Metrics,
                         @transient var cassandraUtil: CassandraUtil = null) {

  lazy private val mapper: ObjectMapper = new ObjectMapper()
  private[this] val logger = LoggerFactory.getLogger(classOf[CertificateService])

  def readCertTemplates(edata: util.Map[String, AnyRef]): util.Map[String, AnyRef] = {
    val selectQuery = QueryBuilder.select().all().from(config.dbKeyspace, config.dbBatchTable)
    selectQuery.where.and(QueryBuilder.eq(config.courseBatchPrimaryKey.head, edata.get(config.batchId).asInstanceOf[String])).
      and(QueryBuilder.eq(config.courseBatchPrimaryKey(1), edata.get(config.courseId).asInstanceOf[String]))
    val rows = cassandraUtil.find(selectQuery.toString)
    metrics.incCounter(config.dbReadCount)
    if (CollectionUtils.isNotEmpty(rows)) {
      rows.asScala.head.getObject(config.certTemplates).asInstanceOf[util.Map[String, AnyRef]]
    } else {
      throw new Exception("Certificate template is not available : " + selectQuery.toString)
    }
  }

  def readAsset(assetId: String): util.Map[String, AnyRef] = {
    val httpResponse = CertificatePreProcessorStreamTask.httpUtil.get(config.contentBaseUrl + "/asset/v4/read/" + assetId)
    if (httpResponse.status == 200) {
      logger.info("Asset read success: " + httpResponse.body)
      val response = mapper.readValue(httpResponse.body, classOf[util.Map[String, AnyRef]])
      val result = response.getOrDefault("result", new util.HashMap()).asInstanceOf[util.Map[String, AnyRef]]
      val content = result.getOrDefault("content", new util.HashMap()).asInstanceOf[util.Map[String, AnyRef]]
      if (MapUtils.isEmpty(content)) {
        throw new Exception("Asset is empty for assetId : " + assetId)
      }
      content
    } else {
      throw new Exception("Asset read failed for assetId : " + assetId + " " + httpResponse.status + " :: " + httpResponse.body)
    }
  }

  def updateCertTemplates(certTemplates: util.Map[String, AnyRef], edata: util.Map[String, AnyRef]): Unit = {
    println("updateCertTemplates called : " + certTemplates)
    val updateQuery = QueryBuilder.update(config.dbKeyspace, config.dbBatchTable)
      .`with`(QueryBuilder.set(config.certTemplates, certTemplates))
      .where(QueryBuilder.eq(config.courseBatchPrimaryKey.head, edata.get(config.batchId).asInstanceOf[String]))
      .and(QueryBuilder.eq(config.courseBatchPrimaryKey(1), edata.get(config.courseId).asInstanceOf[String]))
    val result = cassandraUtil.upsert(updateQuery.toString)
    if (result) {
      metrics.incCounter(config.dbUpdateCount)
    } else {
      throw new Exception("CertTemplate database update has failed : " + updateQuery.toString)
    }
  }

  def readUserIdsFromDb(enrollmentCriteria: util.Map[String, AnyRef], edata: util.Map[String, AnyRef], templateName: String): Unit = {
    println("readUserIdsFromDb called : " + enrollmentCriteria)
    val selectQuery = QueryBuilder.select().all().from(config.dbKeyspace, config.dbUserTable)
    enrollmentCriteria.entrySet().forEach(enrol => {selectQuery.where.and(QueryBuilder.eq(enrol.getKey, enrol.getValue))})
    selectQuery.where.and(QueryBuilder.eq(config.userEnrolmentsPrimaryKey.head, edata.get(config.userIds).asInstanceOf[util.ArrayList[String]])).
      and(QueryBuilder.eq(config.userEnrolmentsPrimaryKey(1), edata.get(config.batchId).asInstanceOf[String])).
      and(QueryBuilder.eq(config.userEnrolmentsPrimaryKey(2), edata.get(config.courseId).asInstanceOf[String])).allowFiltering()
    val rows = cassandraUtil.find(selectQuery.toString)
    metrics.incCounter(config.dbReadCount)
    if (CollectionUtils.isNotEmpty(rows)) {
     val userIds =  rows.asScala.filter(row => {
        val issuedCertificates = row.getList("issued_certificates", TypeTokens.mapOf(classOf[String], classOf[String]))
        val isCertIssued = (CollectionUtils.isNotEmpty(issuedCertificates)) &&
          (CollectionUtils.isNotEmpty(issuedCertificates.asScala.toStream.filter(cert => {
          StringUtils.equalsIgnoreCase(templateName, cert.getOrDefault("name", ""))
        }).toList.asJava))
        row.getBool("active") && (!isCertIssued || edata.containsKey(config.reIssue) && edata.get(config.reIssue).asInstanceOf[Boolean])
      }).map(row => row.getString("userid")).toList
      userIds
    } else {
      throw new Exception("User read failed : " + selectQuery.toString)
    }
  }

  def readUserCertificate(edata: util.Map[String, AnyRef]) {
    val selectQuery = QueryBuilder.select().all().from(config.dbKeyspace, config.dbUserTable)
    selectQuery.where.and(QueryBuilder.eq(config.userEnrolmentsPrimaryKey.head, edata.get(config.userId).asInstanceOf[String])).
      and(QueryBuilder.eq(config.userEnrolmentsPrimaryKey(1), edata.get(config.courseId).asInstanceOf[String])).
      and(QueryBuilder.eq(config.userEnrolmentsPrimaryKey(2), edata.get(config.batchId).asInstanceOf[String]))
    val rows = cassandraUtil.find(selectQuery.toString)
    metrics.incCounter(config.dbReadCount)
    if (CollectionUtils.isNotEmpty(rows)) {
      edata.put(config.issued_certificates, rows.asScala.head.getObject(config.issued_certificates))
      edata.put(config.issuedDate, rows.asScala.head.getDate(config.completedOn))
    } else {
      throw new Exception("User : " + edata.get(config.userId) + " is not enrolled for batch :  "
        + edata.get(config.batchId) + " and course : " + edata.get(config.courseId))
    }
  }

  def readContent(courseId: String, cache: DataCache): util.Map[String, AnyRef] = {
    val httpResponse = CertificatePreProcessorStreamTask.httpUtil.get(config.lmsBaseUrl + "/content/v3/read/" + courseId)
    if (httpResponse.status == 200) {
      logger.info("Content read success: " + httpResponse.body)
      val response = mapper.readValue(httpResponse.body, classOf[util.Map[String, AnyRef]])
      val result = response.getOrDefault("result", new util.HashMap()).asInstanceOf[util.Map[String, AnyRef]]
      val content = result.getOrDefault("content", new util.HashMap()).asInstanceOf[util.Map[String, AnyRef]]
      if (MapUtils.isEmpty(content)) {
        throw new Exception("Content is empty for courseId : " + courseId)
      }
      content
    } else {
      throw new Exception("Content read failed for courseId : " + courseId + " " + httpResponse.status + " :: " + httpResponse.body)
      null
    }
  }

  def getUserDetails(userId: String): util.Map[String, AnyRef] = {
    val httpRequest = s"""{"request":{"filters":{"identifier":"${userId}"},"fields":["firstName", "lastName", "userName", "rootOrgName", "rootOrgId","maskedPhone"]}}"""
    val httpResponse = CertificatePreProcessorStreamTask.httpUtil.post(config.searchBaseUrl + "/private/user/v1/search", httpRequest)
    if (httpResponse.status == 200) {
      logger.info("User search success: " + httpResponse.body)
      val response = mapper.readValue(httpResponse.body, classOf[util.Map[String, AnyRef]])
      val result = response.getOrDefault("result", new util.HashMap()).asInstanceOf[util.Map[String, AnyRef]]
      val contents = result.getOrDefault("content", new util.ArrayList[util.Map[String, AnyRef]]()).asInstanceOf[util.List[util.Map[String, AnyRef]]]
      val userDetails = contents.get(0)
      if (MapUtils.isEmpty(userDetails)) {
        throw new Exception("User not found for userId : " + userId)
      }
      userDetails
    } else {
      throw new Exception("User not found for userId : " + userId + " " + httpResponse.status + " :: " + httpResponse.body)
      null
    }
  }

  def readOrgKeys(rootOrgId: String) : util.Map[String, AnyRef] = {
    val httpRequest = s"""{"request":{"organisationId":"${rootOrgId}"}}}"""
    val httpResponse = CertificatePreProcessorStreamTask.httpUtil.post(config.lmsBaseUrl + "/v1/org/read", httpRequest)
    if (httpResponse.status == 200) {
      logger.info("Org read success: " + httpResponse.body)
      val response = mapper.readValue(httpResponse.body, classOf[util.Map[String, AnyRef]])
      val result = response.getOrDefault("result", new util.HashMap()).asInstanceOf[util.Map[String, AnyRef]]
      val keys = result.getOrDefault("keys", new util.HashMap()).asInstanceOf[util.Map[String, AnyRef]]
      if (MapUtils.isNotEmpty(keys) && CollectionUtils.isNotEmpty(keys.get("signKeys").asInstanceOf[util.List[util.Map[String, AnyRef]]])) {
        val signKeys = new util.HashMap[String, AnyRef]() {{
          put("id", keys.get("signKeys").asInstanceOf[util.List[util.Map[String, AnyRef]]].get(0))
        }}
        signKeys
      }
      keys
    } else {
      throw new Exception("Error while reading organisation  : " + rootOrgId + " " + httpResponse.status + " :: " + httpResponse.body)
      null
    }
  }
}
