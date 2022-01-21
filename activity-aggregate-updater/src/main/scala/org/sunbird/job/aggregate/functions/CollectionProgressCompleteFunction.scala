package org.sunbird.job.aggregate.functions

import java.util.UUID

import com.datastax.driver.core.querybuilder.{QueryBuilder, Select, Update}
import com.google.gson.Gson
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.cache.RedisConnect
import org.sunbird.job.aggregate.common.DeDupHelper
import org.sunbird.job.dedup.DeDupEngine
import org.sunbird.job.{BaseProcessFunction, Metrics}
import org.sunbird.job.aggregate.domain.{ActorObject, CollectionProgress, EventContext, EventData, EventObject, TelemetryEvent}
import org.sunbird.job.aggregate.task.ActivityAggregateUpdaterConfig
import org.sunbird.job.util.CassandraUtil

import scala.collection.JavaConverters._

class CollectionProgressCompleteFunction(config: ActivityAggregateUpdaterConfig)(implicit val enrolmentCompleteTypeInfo: TypeInformation[List[CollectionProgress]], val stringTypeInfo: TypeInformation[String], @transient var cassandraUtil: CassandraUtil = null)
  extends BaseProcessFunction[List[CollectionProgress], String](config) {

  private[this] val logger = LoggerFactory.getLogger(classOf[CollectionProgressCompleteFunction])
  lazy private val gson = new Gson()
  var deDupEngine: DeDupEngine = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    cassandraUtil = new CassandraUtil(config.dbHost, config.dbPort)
    deDupEngine = new DeDupEngine(config, new RedisConnect(config, Option(config.deDupRedisHost), Option(config.deDupRedisPort)), config.deDupStore, config.deDupExpirySec)
    deDupEngine.init()
  }

  override def close(): Unit = {
    cassandraUtil.close()
    deDupEngine.close()
    super.close()
  }

  override def processElement(events: List[CollectionProgress], context: ProcessFunction[List[CollectionProgress], String]#Context, metrics: Metrics): Unit = {
    logger.info("events => "+events)

    val pendingEnrolments = if (config.filterCompletedEnrolments) events.filter {p =>
      val row = getEnrolment(p.userId, p.courseId, p.batchId)(metrics)
      (row != null && row.getInt("status") != 2)
    } else events
    logger.info("pendingEnrolments =>"+pendingEnrolments)

    val enrolmentQueries = pendingEnrolments.map(enrolmentComplete => getEnrolmentCompleteQuery(enrolmentComplete))
    logger.info("enrolmentQueries => "+enrolmentQueries)
    updateDB(config.thresholdBatchWriteSize, enrolmentQueries)(metrics)
    pendingEnrolments.foreach(e => {
      createIssueCertEvent(e, context)(metrics)
      generateAuditEvent(e, context)(metrics)
    })
    logger.info("posting events completed")
    // Create and update the checksum to DeDup store for the input events.
    if (config.dedupEnabled) {
      events.map(cp => cp.inputContents.map(c => DeDupHelper.getMessageId(cp.courseId, cp.batchId, cp.userId, c, 2)))
        .flatten.foreach(checksum => deDupEngine.storeChecksum(checksum))
    }
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
    logger.info("audit event =>"+gson.toJson(auditEvent))
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
      logger.info("is cassandra cluster available =>"+(null !=cassandraUtil.session))
      val result = cassandraUtil.upsert(cqlBatch.toString)
      logger.info("result after update => "+result)
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
    logger.info("o/p event:  "+event)
    context.output(config.certIssueOutputTag, event)
    metrics.incCounter(config.certIssueEventsCount)
  }
}
