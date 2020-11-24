package org.sunbird.job.functions

import java.util.UUID

import com.datastax.driver.core.querybuilder.{QueryBuilder, Select, Update}
import com.google.gson.Gson
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.{BaseProcessFunction, Metrics}
import org.sunbird.job.domain.EnrolmentComplete
import org.sunbird.job.task.ActivityAggregateUpdaterConfig
import org.sunbird.job.util.CassandraUtil

class EnrolmentCompleteFunction(config: ActivityAggregateUpdaterConfig)(implicit val enrolmentCompleteTypeInfo: TypeInformation[List[EnrolmentComplete]], @transient var cassandraUtil: CassandraUtil = null)
  extends BaseProcessFunction[List[EnrolmentComplete], String](config) {

  private[this] val logger = LoggerFactory.getLogger(classOf[EnrolmentCompleteFunction])
  lazy private val gson = new Gson()

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    cassandraUtil = new CassandraUtil(config.dbHost, config.dbPort)
  }

  override def close(): Unit = {
    cassandraUtil.close()
    super.close()
  }

  override def processElement(events: List[EnrolmentComplete], context: ProcessFunction[List[EnrolmentComplete], String]#Context, metrics: Metrics): Unit = {
    val pendingEnrolments = events.filter {p =>
      val row = getEnrolment(p.userId, p.courseId, p.batchId, metrics)
      val status = row.getInt("status")
      status != 2
    }
    
    val enrolmentQueries = pendingEnrolments.map(enrolmentComplete => getEnrolmentCompleteQuery(enrolmentComplete))
    updateDB(config.thresholdBatchWriteSize, enrolmentQueries)(metrics)
    pendingEnrolments.foreach(e => createIssueCertEvent(e, context)(metrics))
  }

  override def metricsList(): List[String] = {
    List(config.dbUpdateCount, config.dbReadCount, config.enrolmentCompleteCount, config.certIssueEventsCount)
  }

  def getEnrolment(userId: String, courseId: String, batchId: String, metrics: Metrics) = {
    val selectWhere: Select.Where = QueryBuilder.select().all()
      .from(config.dbKeyspace, config.dbUserEnrolmentsTable).
      where()
    selectWhere.and(QueryBuilder.eq("userid", userId))
      .and(QueryBuilder.eq("courseid", courseId))
      .and(QueryBuilder.eq("batchid", batchId))
    metrics.incCounter(config.dbReadCount)
    cassandraUtil.findOne(selectWhere.toString)
  }

  def getEnrolmentCompleteQuery(enrolment: EnrolmentComplete): Update.Where = {
    logger.info("Enrolment completed for userId: " + enrolment.userId + " batchId: " + enrolment.batchId)
    QueryBuilder.update(config.dbKeyspace, config.dbUserEnrolmentsTable)
      .`with`(QueryBuilder.set("status", 2))
      .and(QueryBuilder.set("completedon", enrolment.completedOn))
      .and(QueryBuilder.set("progress", enrolment.progress))
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
        metrics.incCounter(config.enrolmentCompleteCount)
      } else {
        val msg = "Database update has failed" + cqlBatch.toString
        logger.error(msg)
        throw new Exception(msg)
      }
    })
  }

  /**
   * Generation of Certificate Issue event for the enrolment completed users to validate and generate certificate.
   * @param enrolment
   * @param context
   * @param metrics
   */
  def createIssueCertEvent(enrolment: EnrolmentComplete, context: ProcessFunction[List[EnrolmentComplete], String]#Context)(implicit metrics: Metrics): Unit = {
    val ets = System.currentTimeMillis
    val mid = s"""LP.${ets}.${UUID.randomUUID}"""
    val event = s"""{"eid": "BE_JOB_REQUEST","ets": ${ets},"mid": "${mid}","actor": {"id": "Course Certificate Generator","type": "System"},"context": {"pdata": {"ver": "1.0","id": "org.sunbird.platform"}},"object": {"id": "${enrolment.batchId}_${enrolment.courseId}","type": "CourseCertificateGeneration"},"edata": {"userIds": ["${enrolment.userId}"],"action": "issue-certificate","iteration": 1, "trigger": "auto-issue","batchId": "${enrolment.batchId}","reIssue": false,"courseId": "${enrolment.courseId}"}}"""
    context.output(config.certIssueOutputTag, event)
    metrics.incCounter(config.certIssueEventsCount)
  }
}
