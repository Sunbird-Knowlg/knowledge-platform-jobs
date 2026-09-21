package org.sunbird.job.knowlg.publish.helpers

import com.datastax.driver.core.querybuilder.QueryBuilder
import org.apache.commons.collections.CollectionUtils
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.sunbird.job.knowlg.task.KnowlgPublishConfig
import org.sunbird.job.publish.core.ObjectData
import org.sunbird.job.util.{CassandraUtil, HttpUtil, JSONUtil}

import java.util
import scala.collection.JavaConverters._

trait AutoBatchCreation {

  private[this] val logger = LoggerFactory.getLogger(classOf[AutoBatchCreation])

  def createBatch(eData: java.util.Map[String, AnyRef], startDate: String)(implicit config: KnowlgPublishConfig, httpUtil: HttpUtil): Unit = {
    val request = new java.util.HashMap[String, AnyRef]() {
      {
        put("request", new java.util.HashMap[String, AnyRef]() {
          {
            put("courseId", eData.get("identifier"))
            put("name", eData.get("name"))
            if (eData.containsKey("createdBy"))
              put("createdBy", eData.get("createdBy"))
            if (eData.containsKey("createdFor"))
              put("createdFor", eData.get("createdFor"))
            put("enrollmentType", "open")
            put("startDate", startDate)
          }
        })
      }
    }
    val httpRequest = JSONUtil.serialize(request)
    val httpResponse = httpUtil.post(config.autoBatchCreateAPIPath, httpRequest)
    if (httpResponse.status == 200) {
      logger.info("Auto batch create success: " + httpResponse.body)
    } else {
      logger.error("Auto batch create failed: " + httpResponse.status + " :: " + httpResponse.body)
      throw new Exception("Auto batch creation failed for " + eData.get("identifier"))
    }
  }

  def getAutoBatchDetails(obj: ObjectData)(implicit cassandraUtil: CassandraUtil, config: KnowlgPublishConfig): util.Map[String, AnyRef] = {
    val identifier = obj.identifier
    if (batchRequired(obj.metadata, identifier)) {
      val createdFor = obj.metadata.get("createdFor")
      new util.HashMap[String, AnyRef]() {
        {
          put("identifier", identifier)
          put("name", obj.metadata.getOrElse("name", "").asInstanceOf[AnyRef])
          if (obj.metadata.contains("createdBy"))
            put("createdBy", obj.metadata("createdBy"))
          if (createdFor.isDefined)
            put("createdFor", createdFor.get)
        }
      }
    } else {
      new util.HashMap[String, AnyRef]()
    }
  }

  def batchRequired(metadata: Map[String, AnyRef], identifier: String)(implicit config: KnowlgPublishConfig, cassandraUtil: CassandraUtil): Boolean = {
    if (isTrackable(metadata, identifier)) !isBatchExists(identifier) else false
  }

  def isTrackable(metadata: Map[String, AnyRef], identifier: String): Boolean = {
    if (metadata.nonEmpty) {
      val trackableStr = metadata.getOrElse("trackable", "{}").asInstanceOf[String]
      val trackableObj = JSONUtil.deserialize[java.util.Map[String, AnyRef]](trackableStr)
      val trackingEnabled = trackableObj.getOrDefault("enabled", "No").asInstanceOf[String]
      val autoBatchCreateEnabled = trackableObj.getOrDefault("autoBatch", "No").asInstanceOf[String]
      val trackable = StringUtils.equalsIgnoreCase(trackingEnabled, "Yes") && StringUtils.equalsIgnoreCase(autoBatchCreateEnabled, "Yes")
      logger.info("Trackable for " + identifier + " : " + trackable)
      trackable
    } else {
      throw new Exception("Metadata [isTrackable] is not found for object: " + identifier)
    }
  }

  def isBatchExists(identifier: String)(implicit config: KnowlgPublishConfig, cassandraUtil: CassandraUtil): Boolean = {
    val selectQuery = QueryBuilder.select().all().from(config.lmsKeyspaceName, config.batchTableName)
    selectQuery.where.and(QueryBuilder.eq("courseid", identifier))
    val rows = cassandraUtil.find(selectQuery.toString)
    if (CollectionUtils.isNotEmpty(rows)) {
      val activeBatches = rows.asScala.filter(row => {
        val enrolmentType = row.getString("enrollmenttype")
        val status = row.getInt("status")
        StringUtils.equalsIgnoreCase(enrolmentType, "Open") && (0 == status || 1 == status)
      }).toList
      if (activeBatches.nonEmpty)
        logger.info("Collection has an active batch: " + activeBatches.head.toString)
      activeBatches.nonEmpty
    } else false
  }

}
