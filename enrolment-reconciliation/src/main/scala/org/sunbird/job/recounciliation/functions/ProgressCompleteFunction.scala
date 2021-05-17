package org.sunbird.job.recounciliation.functions

import com.datastax.driver.core.querybuilder.{QueryBuilder, Select, Update}
import com.google.gson.Gson
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.recounciliation.domain.{ActorObject, CollectionProgress, EventContext, EventData, EventObject, TelemetryEvent}
import org.sunbird.job.recounciliation.task.EnrolmentReconciliationConfig
import org.sunbird.job.util.CassandraUtil
import org.sunbird.job.{BaseProcessFunction, Metrics}

import java.util.UUID
import scala.collection.JavaConverters._

class ProgressCompleteFunction(config: EnrolmentReconciliationConfig)(implicit val enrolmentCompleteTypeInfo: TypeInformation[List[CollectionProgress]], val stringTypeInfo: TypeInformation[String], @transient var cassandraUtil: CassandraUtil = null)
  extends BaseProcessFunction[List[CollectionProgress], String](config) {

  private[this] val logger = LoggerFactory.getLogger(classOf[ProgressCompleteFunction])
  lazy private val gson = new Gson()

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    cassandraUtil = new CassandraUtil(config.dbHost, config.dbPort)
  }

  override def close(): Unit = {
    cassandraUtil.close()
    super.close()
  }

  override def processElement(events: List[CollectionProgress], context: ProcessFunction[List[CollectionProgress], String]#Context, metrics: Metrics): Unit = {
    val pendingEnrolments = if (config.filterCompletedEnrolments) events.filter {p =>
      val row = getEnrolment(p.userId, p.courseId, p.batchId)(metrics)
      (row != null && row.getInt("status") != 2)
    } else events
    
    val enrolmentQueries = pendingEnrolments.map(enrolmentComplete => getEnrolmentCompleteQuery(enrolmentComplete))
    updateDB(config.thresholdBatchWriteSize, enrolmentQueries)(metrics)
    pendingEnrolments.foreach(e => {
      createIssueCertEvent(e, context)(metrics)
      generateAuditEvent(e, context)(metrics)
    })
  }

  override def metricsList(): List[String] = {
    List(config.dbReadCount, config.dbUpdateCount, config.enrolmentCompleteCount, config.certIssueEventsCount)
  }

  def generateAuditEvent(data: CollectionProgress, context: ProcessFunction[List[CollectionProgress], String]#Context)(implicit metrics: Metrics) = {
    val auditEvent = TelemetryEvent(
      actor = ActorObject(id = data.userId),
      edata = EventData(props = Array("status", "completedon"), `type` = "enrol-complete"), // action values are "start", "complete".
      context = EventContext(cdata = Array(Map("type" -> config.courseBatch, "id" -> data.batchId).asJava, Map("type" -> "Course", "id" -> data.courseId).asJava)),
      `object` = EventObject(id = data.userId, `type` = "User", rollup = Map[String, String]("l1" -> data.courseId).asJava)
    )
    context.output(config.auditEventOutputTag, gson.toJson(auditEvent))

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

  def getEnrolmentCompleteQuery(enrolment: CollectionProgress): Update.Where = {
    logger.info("Enrolment completed for userId: " + enrolment.userId + " batchId: " + enrolment.batchId)
    QueryBuilder.update(config.dbKeyspace, config.dbUserEnrolmentsTable)
      .`with`(QueryBuilder.set("status", 2))
      .and(QueryBuilder.set("completedon", enrolment.completedOn))
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
  def createIssueCertEvent(enrolment: CollectionProgress, context: ProcessFunction[List[CollectionProgress], String]#Context)(implicit metrics: Metrics): Unit = {
    val ets = System.currentTimeMillis
    val mid = s"""LP.${ets}.${UUID.randomUUID}"""
    val event = s"""{"eid": "BE_JOB_REQUEST","ets": ${ets},"mid": "${mid}","actor": {"id": "Course Certificate Generator","type": "System"},"context": {"pdata": {"ver": "1.0","id": "org.sunbird.platform"}},"object": {"id": "${enrolment.batchId}_${enrolment.courseId}","type": "CourseCertificateGeneration"},"edata": {"userIds": ["${enrolment.userId}"],"action": "issue-certificate","iteration": 1, "trigger": "auto-issue","batchId": "${enrolment.batchId}","reIssue": false,"courseId": "${enrolment.courseId}"}}"""
    context.output(config.certIssueOutputTag, event)
    metrics.incCounter(config.certIssueEventsCount)
  }
}
