package org.sunbird.job.functions

import java.util

import org.apache.commons.collections.{CollectionUtils, MapUtils}
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper
import org.slf4j.LoggerFactory
import org.sunbird.job.Metrics
import org.sunbird.job.cache.DataCache
import org.sunbird.job.task.CollectionCompletePostProcessorConfig
import org.sunbird.job.util.HttpUtil

import scala.collection.JavaConverters._

object CertificateApiService {

  lazy private val mapper: ObjectMapper = new ObjectMapper()
  var httpUtil = new HttpUtil
  private[this] val logger = LoggerFactory.getLogger(CertificateApiService.getClass)

  def getUsersFromUserCriteria(userCriteria: util.Map[String, AnyRef], userIds: List[String])(implicit config: CollectionCompletePostProcessorConfig): List[String] = {
    val batchSize = 50
    val batchList = userIds.grouped(batchSize).toList
    logger.info("getUsersFromUserCriteria called : " + batchList)
    batchList.flatMap(batch => {
      val httpRequest = s"""{"request":{"filters":{"identifier":"${batch}, ${userCriteria}"},"fields":["identifier"]}}"""
      val httpResponse = httpUtil.post(config.learnerBasePath + config.userV1Search, httpRequest)
      if (httpResponse.status == 200) {
        logger.info("User search success: " + httpResponse.body)
        val response = mapper.readValue(httpResponse.body, classOf[util.Map[String, AnyRef]])
        val result = response.getOrDefault("result", new util.HashMap()).asInstanceOf[util.Map[String, AnyRef]]
        val contents = result.getOrDefault("content", new util.ArrayList[util.Map[String, AnyRef]]()).asInstanceOf[util.ArrayList[util.Map[String, AnyRef]]]

        logger.info("getUsersFromUserCriteria : contents from search response : " + contents)
        val userList = contents.asScala.map(content => content.getOrDefault(config.identifier, "").asInstanceOf[String]).toList
        if (userList.isEmpty) throw new Exception("User not found for userCriteria : " + userCriteria)
        logger.info("getUsersFromUserCriteria : User found Batch : " + userList)

        userList
      } else throw new Exception("Search users for given criteria failed to fetch data : " + userCriteria + " " + httpResponse.status + " :: " + httpResponse.body)
    })
  }

  def readContent(courseId: String, collectionCache: DataCache)
                 (implicit config: CollectionCompletePostProcessorConfig, metrics: Metrics): util.Map[String, AnyRef] = {
    logger.info("readContent called : courseId : " + courseId)
    val courseData = collectionCache.getWithRetry(courseId)
    logger.info("Data from content cache for courseId : {} :" + courseData, courseId)
    metrics.incCounter(config.cacheReadCount)
    if (courseData.nonEmpty) {
      logger.info("readContent cache called : courseData : " + courseId)
      courseData.asJava
    } else {
      val httpResponse = httpUtil.get(config.contentBaseUrl + config.contentV3Read + courseId)
      if (httpResponse.status == 200) {
        logger.info("Content read success: " + httpResponse.body)
        val response = mapper.readValue(httpResponse.body, classOf[util.Map[String, AnyRef]])
        val result = response.getOrDefault("result", new util.HashMap()).asInstanceOf[util.Map[String, AnyRef]]
        val content = result.getOrDefault("content", new util.HashMap()).asInstanceOf[util.Map[String, AnyRef]]
        if (MapUtils.isEmpty(content)) throw new Exception("Content is empty for courseId : " + courseId)
        content
      } else throw new Exception("Content read failed for courseId : " + courseId + " " + httpResponse.status + " :: " + httpResponse.body)
    }
  }

  def getUserDetails(userId: String)(implicit config: CollectionCompletePostProcessorConfig): util.Map[String, AnyRef] = {
    val httpRequest = s"""{"request":{"filters":{"identifier":"${userId}"},"fields":["firstName", "lastName", "userName", "rootOrgName", "rootOrgId","maskedPhone"]}}"""
    val httpResponse = httpUtil.post(config.learnerBasePath + config.userV1Search, httpRequest)
    if (httpResponse.status == 200) {
      logger.info("User search success: " + httpResponse.body)
      val response = mapper.readValue(httpResponse.body, classOf[util.Map[String, AnyRef]])
      val result = response.getOrDefault("result", new util.HashMap()).asInstanceOf[util.Map[String, AnyRef]]
      val responseBody = result.getOrDefault("response", new util.HashMap()).asInstanceOf[util.Map[String, AnyRef]]
      val contents = responseBody.getOrDefault("content", new util.ArrayList[util.Map[String, AnyRef]]()).asInstanceOf[util.List[util.Map[String, AnyRef]]]
      val userDetails = contents.get(0)
      if (MapUtils.isEmpty(userDetails))
        throw new Exception("User not found for userId : " + userId)
      userDetails
    } else throw new Exception("User not found for userId : " + userId + " " + httpResponse.status + " :: " + httpResponse.body)
  }

  def readOrgKeys(rootOrgId: String)(implicit config: CollectionCompletePostProcessorConfig): util.Map[String, AnyRef] = {
    val httpRequest = s"""{"request":{"organisationId":"${rootOrgId}"}}}"""
    val httpResponse = httpUtil.post(config.learnerBasePath + config.orgV1Read, httpRequest)
    if (httpResponse.status == 200) {
      logger.info("Org read success: " + httpResponse.body)
      val response = mapper.readValue(httpResponse.body, classOf[util.Map[String, AnyRef]])
      val result = response.getOrDefault("result", new util.HashMap()).asInstanceOf[util.Map[String, AnyRef]]
      val responseMap = result.getOrDefault("response", new util.HashMap()).asInstanceOf[util.Map[String, AnyRef]]
      val keys = responseMap.getOrDefault("keys", new util.HashMap()).asInstanceOf[util.Map[String, AnyRef]]
      if (MapUtils.isNotEmpty(keys) && CollectionUtils.isNotEmpty(keys.get("signKeys").asInstanceOf[util.List[String]])) {
        val signKeys = new util.HashMap[String, AnyRef]() {
          {
            put("id", keys.get("signKeys").asInstanceOf[util.List[String]].get(0))
          }
        }
        signKeys
      } else keys
    } else new util.HashMap[String, AnyRef]()
  }
}
