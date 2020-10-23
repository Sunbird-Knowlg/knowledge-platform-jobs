package org.sunbird.job.functions

import java.util

import com.datastax.driver.core.Row
import com.datastax.driver.core.querybuilder.QueryBuilder
import org.apache.commons.collections.CollectionUtils
import org.sunbird.job.Metrics
import org.sunbird.job.task.CertificatePreProcessorConfig
import org.sunbird.job.util.CassandraUtil

import scala.collection.JavaConverters._

object CertificateDbService {

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

  def readUserIdsFromDb(enrollmentCriteria: util.Map[String, AnyRef], edata: util.Map[String, AnyRef])
                       (implicit metrics: Metrics, @transient cassandraUtil: CassandraUtil,
                        config: CertificatePreProcessorConfig): util.List[Row] = {
    println("readUserIdsFromDb called : " + enrollmentCriteria)
    val selectQuery = QueryBuilder.select().all().from(config.dbKeyspace, config.dbUserTable)
    selectQuery.where.and(QueryBuilder.in(config.userEnrolmentsPrimaryKey.head, edata.get(config.userIds).asInstanceOf[util.ArrayList[String]])).
      and(QueryBuilder.eq(config.userEnrolmentsPrimaryKey(1), edata.get(config.courseId).asInstanceOf[String])).
      and(QueryBuilder.eq(config.userEnrolmentsPrimaryKey(2), edata.get(config.batchId).asInstanceOf[String]))
    enrollmentCriteria.asScala.map(criteria => selectQuery.where.and(QueryBuilder.eq(criteria._1, criteria._2)))
    selectQuery.allowFiltering()
    println("readUserIdsFromDb : Enroll User read query : " + selectQuery.toString)
    val rows = cassandraUtil.find(selectQuery.toString)
    metrics.incCounter(config.dbReadCount)
    if (CollectionUtils.isNotEmpty(rows))
      rows
    else
      throw new Exception("User read failed : " + selectQuery.toString)
  }

  def fetchAssessedUsersFromDB(edata: util.Map[String, AnyRef])
                              (implicit metrics: Metrics, @transient cassandraUtil: CassandraUtil,
                               config: CertificatePreProcessorConfig): util.List[Row] = {
    val query = "SELECT user_id, max(total_score) as score, total_max_score FROM " + config.dbKeyspace +
      "." + config.dbAssessmentAggregator + " where course_id='" + edata.get(config.courseId).asInstanceOf[String] + "' AND batch_id='" +
      edata.get(config.batchId).asInstanceOf[String] + "' " + "GROUP BY course_id,batch_id,user_id,content_id ORDER BY batch_id,user_id,content_id;"
    println("fetchAssessedUsersFromDB called query : " + query)
    val rows = cassandraUtil.find(query)
    metrics.incCounter(config.dbReadCount)
    if (CollectionUtils.isNotEmpty(rows))
      rows
    else
      throw new Exception("Assess User read failed : " + query)
  }

  def readUserCertificate(edata: util.Map[String, AnyRef])
                         (implicit metrics: Metrics, @transient cassandraUtil: CassandraUtil, config: CertificatePreProcessorConfig): Map[String, AnyRef] = {
    println("readUserCertificate called edata : " + edata)
    val selectQuery = QueryBuilder.select().all().from(config.dbKeyspace, config.dbUserTable)
    selectQuery.where.and(QueryBuilder.in(config.userEnrolmentsPrimaryKey.head, edata.get(config.userIds).asInstanceOf[util.ArrayList[String]])).
      and(QueryBuilder.eq(config.userEnrolmentsPrimaryKey(1), edata.get(config.courseId).asInstanceOf[String])).
      and(QueryBuilder.eq(config.userEnrolmentsPrimaryKey(2), edata.get(config.batchId).asInstanceOf[String]))
    selectQuery.allowFiltering()
    println("readUserCertificate query : " + selectQuery.toString)
    val rows = cassandraUtil.find(selectQuery.toString)
    metrics.incCounter(config.dbReadCount)
    if (CollectionUtils.isNotEmpty(rows)) {
      Map(config.issued_certificates -> rows.asScala.head.getObject(config.issued_certificates),
        config.issuedDate -> rows.asScala.head.getDate(config.completedOn))
    } else {
      throw new Exception("User : " + edata.get(config.userId) + " is not enrolled for batch :  "
        + edata.get(config.batchId) + " and course : " + edata.get(config.courseId))
    }
  }
}
