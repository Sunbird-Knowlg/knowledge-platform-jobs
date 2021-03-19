package org.sunbird.job.functions

import com.datastax.driver.core.querybuilder.{QueryBuilder, Select, Update}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.domain._
import org.sunbird.job.task.EnrolmentReconciliationConfig
import org.sunbird.job.util.CassandraUtil
import org.sunbird.job.{BaseProcessFunction, Metrics}

import scala.collection.JavaConverters._

class ProgressUpdateFunction(config: EnrolmentReconciliationConfig)(implicit val enrolmentCompleteTypeInfo: TypeInformation[List[CollectionProgress]], val stringTypeInfo: TypeInformation[String], @transient var cassandraUtil: CassandraUtil = null)
  extends BaseProcessFunction[List[CollectionProgress], String](config) {

  private[this] val logger = LoggerFactory.getLogger(classOf[ProgressUpdateFunction])

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    cassandraUtil = new CassandraUtil(config.dbHost, config.dbPort)
  }

  override def close(): Unit = {
    cassandraUtil.close()
    super.close()
  }

  override def processElement(events: List[CollectionProgress], context: ProcessFunction[List[CollectionProgress], String]#Context, metrics: Metrics): Unit = {
    val pendingEnrolments = if (config.filterCompletedEnrolments) events.filter { p =>
      val row = getEnrolment(p.userId, p.courseId, p.batchId)(metrics)
      (row != null && row.getInt("status") != 2)
    } else events
    val enrolmentQueries = pendingEnrolments.map(collectionProgress => getEnrolmentUpdateQuery(collectionProgress))
    updateDB(config.thresholdBatchWriteSize, enrolmentQueries)(metrics)
    // Create and update the checksum to DeDup store for the input events.
  }

  override def metricsList(): List[String] = {
    List(config.dbReadCount, config.dbUpdateCount)
  }

  def getEnrolment(userId: String, courseId: String, batchId: String)(implicit metrics: Metrics) = {
    val selectWhere: Select.Where = QueryBuilder.select().all()
      .from(config.dbKeyspace, config.dbUserEnrolmentsTable).
      where()
    selectWhere.and(QueryBuilder.eq("userid", userId))
      .and(QueryBuilder.eq("courseid", courseId))
      .and(QueryBuilder.eq("batchid", batchId))
    metrics.incCounter(config.dbReadCount)
    cassandraUtil.findOne(selectWhere.toString)
  }

  def getEnrolmentUpdateQuery(enrolment: CollectionProgress): Update.Where = {
    logger.info("Enrolment updated for userId: " + enrolment.userId + " batchId: " + enrolment.batchId)
    QueryBuilder.update(config.dbKeyspace, config.dbUserEnrolmentsTable)
      .`with`(QueryBuilder.set("status", 1))
      .and(QueryBuilder.set("progress", enrolment.progress))
      .and(QueryBuilder.set("contentstatus", enrolment.contentStatus.asJava))
      .and(QueryBuilder.set("datetime", System.currentTimeMillis))
      .where(QueryBuilder.eq("userid", enrolment.userId))
      .and(QueryBuilder.eq("courseid", enrolment.courseId))
      .and(QueryBuilder.eq("batchid", enrolment.batchId))
  }

  /**
   * Method to update the specific table in a batch format.
   */
  def updateDB(batchSize: Int, queriesList: List[Update.Where])(implicit metrics: Metrics): Unit = {
    val groupedQueries = queriesList.grouped(batchSize).toList
    groupedQueries.foreach(queries => {
      val cqlBatch = QueryBuilder.batch()
      queries.map(query => cqlBatch.add(query))
      val result = cassandraUtil.upsert(cqlBatch.toString)
      if (result) {
        metrics.incCounter(config.dbUpdateCount)
      } else {
        val msg = "Database update has failed" + cqlBatch.toString
        logger.error(msg)
        throw new Exception(msg)
      }
    })
  }
}
