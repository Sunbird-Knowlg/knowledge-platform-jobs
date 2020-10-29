package org.sunbird.job.functions

import java.text.SimpleDateFormat
import java.util

import com.datastax.driver.core.querybuilder.QueryBuilder
import org.apache.commons.collections.CollectionUtils
import org.sunbird.job.Metrics
import org.sunbird.job.task.CertificatePreProcessorConfig
import org.sunbird.job.util.CassandraUtil

import scala.collection.JavaConverters
import scala.collection.JavaConverters._

object CertificateDbService {

  val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");

  def readCertTemplates(edata: util.Map[String, AnyRef])
                       (implicit metrics: Metrics, @transient cassandraUtil: CassandraUtil,
                        config: CertificatePreProcessorConfig): util.Map[String, AnyRef] = {
    println("readCertTemplates called : ")
    val selectQuery = QueryBuilder.select().all().from(config.dbKeyspace, config.dbBatchTable)
    selectQuery.where.and(QueryBuilder.eq(config.courseBatchPrimaryKey.head, edata.get(config.courseId).asInstanceOf[String])).
      and(QueryBuilder.eq(config.courseBatchPrimaryKey(1), edata.get(config.batchId).asInstanceOf[String]))
    println("readCertTemplates query : " + selectQuery.toString)
    val rows = cassandraUtil.find(selectQuery.toString)
    metrics.incCounter(config.dbReadCount)
    if (CollectionUtils.isNotEmpty(rows))
      rows.asScala.head.getObject(config.certTemplates).asInstanceOf[util.Map[String, AnyRef]]
    else
      throw new Exception("Certificate template is not available : " + selectQuery.toString)
  }

  def readUserIdsFromDb(enrollmentCriteria: util.Map[String, AnyRef], edata: util.Map[String, AnyRef], templateName: String)
                       (implicit metrics: Metrics, @transient cassandraUtil: CassandraUtil,
                        config: CertificatePreProcessorConfig): List[String] = {
    println("readUserIdsFromDb called : " + enrollmentCriteria)
    val selectQuery = QueryBuilder.select().all().from(config.dbKeyspace, config.dbUserTable)
    selectQuery.where.and(QueryBuilder.in(config.userEnrolmentsPrimaryKey.head, edata.get(config.userIds).asInstanceOf[util.List[String]])).
      and(QueryBuilder.eq(config.userEnrolmentsPrimaryKey(1), edata.get(config.courseId).asInstanceOf[String])).
      and(QueryBuilder.eq(config.userEnrolmentsPrimaryKey(2), edata.get(config.batchId).asInstanceOf[String]))
    enrollmentCriteria.asScala.map(criteria => selectQuery.where.and(QueryBuilder.eq(criteria._1, criteria._2)))
    selectQuery.allowFiltering()
    println("readUserIdsFromDb : Enroll User read query : " + selectQuery.toString)
    val rows = cassandraUtil.find(selectQuery.toString)
    metrics.incCounter(config.dbReadCount)
    if (CollectionUtils.isNotEmpty(rows))
      IssueCertificateUtil.getActiveUserIds(rows, edata, templateName)
    else
      throw new Exception("User read failed : " + selectQuery.toString)
  }

  def fetchAssessedUsersFromDB(edata: util.Map[String, AnyRef], assessmentCriteria: util.Map[String, AnyRef])
                              (implicit metrics: Metrics, @transient cassandraUtil: CassandraUtil,
                               config: CertificatePreProcessorConfig): List[String] = {
    val query = "SELECT user_id, max(total_score) as score, total_max_score FROM " + config.dbKeyspace +
      "." + config.dbAssessmentAggregator + " where course_id='" + edata.get(config.courseId).asInstanceOf[String] + "' AND batch_id='" +
      edata.get(config.batchId).asInstanceOf[String] + "' " + "GROUP BY course_id,batch_id,user_id,content_id ORDER BY batch_id,user_id,content_id;"
    println("fetchAssessedUsersFromDB called query : " + query)
    val rows = cassandraUtil.find(query)
    metrics.incCounter(config.dbReadCount)
    if (CollectionUtils.isNotEmpty(rows))
      IssueCertificateUtil.getAssessedUserIds(rows, assessmentCriteria, edata)
    else
      throw new Exception("Assess User read failed : " + query)
  }

  def readUserCertificate(edata: util.Map[String, AnyRef])
                         (implicit metrics: Metrics, @transient cassandraUtil: CassandraUtil, config: CertificatePreProcessorConfig): util.Map[String, AnyRef] = {
    println("readUserCertificate called edata : " + edata)
    val selectQuery = QueryBuilder.select().all().from(config.dbKeyspace, config.dbUserTable)
    selectQuery.where.and(QueryBuilder.eq(config.userEnrolmentsPrimaryKey.head, edata.get(config.userId).asInstanceOf[String])).
      and(QueryBuilder.eq(config.userEnrolmentsPrimaryKey(1), edata.get(config.courseId).asInstanceOf[String])).
      and(QueryBuilder.eq(config.userEnrolmentsPrimaryKey(2), edata.get(config.batchId).asInstanceOf[String]))
    println("readUserCertificate query : " + selectQuery.toString)
    val rows = cassandraUtil.find(selectQuery.toString)
    println("readUserCertificate count : " + rows.size())
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
