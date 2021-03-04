package org.sunbird.job.functions

import java.text.SimpleDateFormat
import java.util

import com.datastax.driver.core.TypeTokens
import com.datastax.driver.core.querybuilder.QueryBuilder
import com.google.common.reflect.TypeToken
import org.apache.commons.collections.CollectionUtils
import org.slf4j.LoggerFactory
import org.sunbird.collectioncomplete.domain.Event
import org.sunbird.job.Metrics
import org.sunbird.job.task.CollectionCompletePostProcessorConfig
import org.sunbird.job.util.CassandraUtil

import scala.collection.JavaConverters
import scala.collection.JavaConverters._

object CertificateDbService {

  val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
  private[this] val logger = LoggerFactory.getLogger(CertificateDbService.getClass)

  def readCertTemplates(batchId: String, courseId: String)
                       (implicit metrics: Metrics, @transient cassandraUtil: CassandraUtil,
                        config: CollectionCompletePostProcessorConfig): Map[String, Map[String, String]] = {
    logger.info("readCertTemplates called : ")
    val selectQuery = QueryBuilder.select().all().from(config.dbKeyspace, config.dbBatchTable).where(QueryBuilder.eq(config.courseBatchPrimaryKey.head, courseId)).
      and(QueryBuilder.eq(config.courseBatchPrimaryKey(1), batchId))
    logger.info("readCertTemplates query : " + selectQuery.toString)
    val row = cassandraUtil.findOne(selectQuery.toString)
    if (null != row) {
      metrics.incCounter(config.dbReadCount)
      val certTemplates = row.getMap(config.certTemplates, TypeToken.of(classOf[String]),
        TypeTokens.mapOf(classOf[String], classOf[String]))
      certTemplates.asScala.map(temp => (temp._1, temp._2.asScala.toMap)).toMap
    }
    else 
      throw new Exception("Certificate template is not available : " + selectQuery.toString)
  }

  def readUserIdsFromDb(enrollmentCriteria: util.Map[String, AnyRef], event: Event, templateName: String)
                       (implicit metrics: Metrics, @transient cassandraUtil: CassandraUtil,
                        config: CollectionCompletePostProcessorConfig): List[String] = {
    logger.info("readUserIdsFromDb called : " + enrollmentCriteria)
    val selectQuery = QueryBuilder.select().all().from(config.dbKeyspace, config.dbUserTable).where.and(QueryBuilder.in(config.userEnrolmentsPrimaryKey.head, event.userIds)).
      and(QueryBuilder.eq(config.userEnrolmentsPrimaryKey(1), event.courseId)).
      and(QueryBuilder.eq(config.userEnrolmentsPrimaryKey(2), event.batchId))
    logger.info("readUserIdsFromDb : Enroll User read query : " + selectQuery.getQueryString())
    val rows = cassandraUtil.find(selectQuery.toString)
    metrics.incCounter(config.dbReadCount)
    if (CollectionUtils.isNotEmpty(rows)) {
      val filteredRows = rows.asScala.toList.filter(row => enrollmentCriteria.getOrDefault("status", 2.asInstanceOf[AnyRef]).asInstanceOf[Int] == row.getInt("status"))
      IssueCertificateUtil.getActiveUserIds(filteredRows, event, templateName)
    }
    else {
      logger.info("No users enrolled with userId :{}, courseid: {}, batchid: {}", event.userIds, event.courseId, event.batchId)
      List()
    }

  }

  def fetchAssessedUsersFromDB(event: Event, assessmentCriteria: util.Map[String, AnyRef], enrolledUsers: List[String])
                              (implicit metrics: Metrics, @transient cassandraUtil: CassandraUtil,
                               config: CollectionCompletePostProcessorConfig): List[String] = {
    val query = "SELECT user_id, max(total_score) as score, total_max_score FROM " + config.dbKeyspace +
      "." + config.dbAssessmentAggregator + " where course_id=? AND batch_id=? AND user_id in ? " +
      "GROUP BY user_id,course_id,batch_id,content_id"
    val userIds = JavaConverters.seqAsJavaList(enrolledUsers.toSeq)
    logger.info("fetchAssessedUsersFromDB called query for courseid:{} batchid:{} userid:{} : " + query, event.courseId, event.batchId, userIds)
    val rows = cassandraUtil.executePreparedStatement(query, event.courseId, event.batchId, userIds)
    metrics.incCounter(config.dbReadCount)
    if (CollectionUtils.isNotEmpty(rows))
      IssueCertificateUtil.getAssessedUserIds(rows, assessmentCriteria, event)
    else {
      logger.info("No Assessment details for courseid: " +event.courseId +"\tbatchid: " + event.batchId + " \t userid: "+ enrolledUsers + " : ")
      List()
    }
  }

  def readUserCertificate(edata: util.Map[String, AnyRef])
                         (implicit metrics: Metrics, @transient cassandraUtil: CassandraUtil, config: CollectionCompletePostProcessorConfig): util.Map[String, AnyRef] = {
    logger.info("readUserCertificate called edata : " + edata)
    val selectQuery = QueryBuilder.select().all().from(config.dbKeyspace, config.dbUserTable).where.and(QueryBuilder.eq(config.userEnrolmentsPrimaryKey.head, edata.get(config.userId).asInstanceOf[String])).
      and(QueryBuilder.eq(config.userEnrolmentsPrimaryKey(1), edata.get(config.courseId).asInstanceOf[String])).
      and(QueryBuilder.eq(config.userEnrolmentsPrimaryKey(2), edata.get(config.batchId).asInstanceOf[String]))
    logger.info("readUserCertificate query : " + selectQuery.toString)
    val rows = cassandraUtil.find(selectQuery.toString)
    metrics.incCounter(config.dbReadCount)
    if (CollectionUtils.isNotEmpty(rows)) {
      JavaConverters.mapAsJavaMap(Map(config.issued_certificates -> rows.asScala.head.getObject(config.issued_certificates),
        config.issuedDate -> simpleDateFormat.format(rows.asScala.head.getTimestamp(config.completedOn))))
    } else {
      throw new Exception("User : " + edata.get(config.userId) + " is not enrolled for batch :  "
        + edata.get(config.batchId) + " and course : " + edata.get(config.courseId))
    }
  }
}
