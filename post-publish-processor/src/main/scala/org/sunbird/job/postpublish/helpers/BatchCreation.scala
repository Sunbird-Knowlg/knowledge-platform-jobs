package org.sunbird.job.postpublish.helpers

import com.datastax.driver.core.querybuilder.QueryBuilder
import org.apache.commons.collections.{CollectionUtils, MapUtils}
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.sunbird.job.task.PostPublishProcessorConfig
import org.sunbird.job.util.{CassandraUtil, JSONUtil}

import scala.collection.JavaConverters._

trait BatchCreation {

  private[this] val logger = LoggerFactory.getLogger(classOf[BatchCreation])

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
      logger.info("Trackable for " +identifier + " : " + trackable)
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

}
