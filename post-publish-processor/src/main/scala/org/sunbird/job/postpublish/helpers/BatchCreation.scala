package org.sunbird.job.postpublish.helpers

import com.datastax.driver.core.querybuilder.QueryBuilder
import org.apache.commons.collections.{CollectionUtils, MapUtils}
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.sunbird.job.postpublish.task.PostPublishProcessorConfig
import org.sunbird.job.util.{CassandraUtil, HttpUtil, JSONUtil, Neo4JUtil}

import java.util
import scala.collection.JavaConverters._

trait BatchCreation {

  private[this] val logger = LoggerFactory.getLogger(classOf[BatchCreation])

  def createBatch(eData: java.util.Map[String, AnyRef], startDate: String)(implicit config: PostPublishProcessorConfig, httpUtil: HttpUtil) = {
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
    val httpResponse = httpUtil.post(config.batchCreateAPIPath, httpRequest)
    if (httpResponse.status == 200) {
      logger.info("Batch create success: " + httpResponse.body)
    } else {
      logger.error("Batch create failed: " + httpResponse.status + " :: " + httpResponse.body)
      throw new Exception("Batch creation failed for " + eData.get("identifier"))
    }
  }


  def batchRequired(metadata: java.util.Map[String, AnyRef], identifier: String)(implicit config: PostPublishProcessorConfig, cassandraUtil: CassandraUtil): Boolean = {
    val trackable = isTrackable(metadata, identifier)
    if (trackable) {
      !isBatchExists(identifier)
    } else false
  }

  def isTrackable(metadata: java.util.Map[String, AnyRef], identifier: String): Boolean = {
    if (MapUtils.isNotEmpty(metadata)) {
      val trackableStr = metadata.getOrDefault("trackable", "{}").asInstanceOf[String]
      val trackableObj = JSONUtil.deserialize[java.util.Map[String, AnyRef]](trackableStr)
      val trackingEnabled = trackableObj.getOrDefault("enabled", "No").asInstanceOf[String]
      val autoBatchCreateEnabled = trackableObj.getOrDefault("autoBatch", "No").asInstanceOf[String]
      val trackable = (StringUtils.equalsIgnoreCase(trackingEnabled, "Yes") && StringUtils.equalsIgnoreCase(autoBatchCreateEnabled, "Yes"))
      logger.info("Trackable for " + identifier + " : " + trackable)
      trackable
    } else {
      throw new Exception("Metadata [isTrackable] is not found for object: " + identifier)
    }
  }

  def isBatchExists(identifier: String)(implicit config: PostPublishProcessorConfig, cassandraUtil: CassandraUtil): Boolean = {
    val selectQuery = QueryBuilder.select().all().from(config.lmsKeyspaceName, config.batchTableName)
    selectQuery.where.and(QueryBuilder.eq("courseid", identifier))
    val rows = cassandraUtil.find(selectQuery.toString)
    if (CollectionUtils.isNotEmpty(rows)) {
      val activeBatches = rows.asScala.filter(row => {
        val enrolmentType = row.getString("enrollmenttype")
        val status = row.getInt("status")
        (StringUtils.equalsIgnoreCase(enrolmentType, "Open") && (0 == status || 1 == status))
      }).toList
      if (activeBatches.nonEmpty)
        logger.info("Collection has a active batch: " + activeBatches.head.toString)
      activeBatches.nonEmpty
    } else false
  }

  def getBatchDetails(identifier: String)(implicit neo4JUtil: Neo4JUtil, cassandraUtil: CassandraUtil, config: PostPublishProcessorConfig): util.Map[String, AnyRef] = {
    logger.info("Process Batch Creation for content: " + identifier)
    val metadata = neo4JUtil.getNodeProperties(identifier)

    // Validate and trigger batch creation.
    if (batchRequired(metadata, identifier)(config, cassandraUtil)) {
      val createdFor = metadata.get("createdFor").asInstanceOf[java.util.List[String]]
      new util.HashMap[String, AnyRef]() {
        {
          put("identifier", identifier)
          put("name", metadata.get("name"))
          put("createdBy", metadata.get("createdBy"))
          if (CollectionUtils.isNotEmpty(createdFor))
            put("createdFor", new util.ArrayList[String](createdFor))
        }
      }
    } else {
      new util.HashMap[String, AnyRef]()
    }
  }

}
