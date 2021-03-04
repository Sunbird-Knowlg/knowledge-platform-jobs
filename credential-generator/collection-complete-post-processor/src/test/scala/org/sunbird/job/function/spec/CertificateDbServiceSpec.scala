package org.sunbird.job.function.spec

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

import com.typesafe.config.{Config, ConfigFactory}
import org.cassandraunit.CQLDataLoader
import org.cassandraunit.dataset.cql.FileCQLDataSet
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.sunbird.collectioncomplete.domain.Event
import org.sunbird.job.Metrics
import org.sunbird.job.functions.CertificateDbService
import org.sunbird.job.task.CollectionCompletePostProcessorConfig
import org.sunbird.job.util.CassandraUtil
import org.sunbird.spec.{BaseMetricsReporter, BaseTestSpec}

import scala.collection.JavaConverters._

class CertificateDbServiceSpec extends BaseTestSpec {

  var cassandraUtil: CassandraUtil = _
  val config: Config = ConfigFactory.load("test.conf")
  val jobConfig: CollectionCompletePostProcessorConfig = new CollectionCompletePostProcessorConfig(config)
  val metrics = Metrics(new ConcurrentHashMap[String, AtomicLong]() {
    {
      put(jobConfig.dbReadCount, new AtomicLong())
    }
  })

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    EmbeddedCassandraServerHelper.startEmbeddedCassandra(80000L)
    cassandraUtil = new CassandraUtil(jobConfig.dbHost, jobConfig.dbPort)
    val session = cassandraUtil.session

    val dataLoader = new CQLDataLoader(session);
    dataLoader.load(new FileCQLDataSet(getClass.getResource("/test.cql").getPath, true, true));
    // Clear the metrics
    testCassandraUtil(cassandraUtil)
    BaseMetricsReporter.gaugeMetrics.clear()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    try {
      EmbeddedCassandraServerHelper.cleanEmbeddedCassandra()
    } catch {
      case ex: Exception => {
      }
    }
  }

  it should "readCertTemplates from courseBatch table" in {
    val event = Map[String, String](jobConfig.batchId -> "0131000245281587206", jobConfig.courseId -> "do_11309999837886054415")
    val map = CertificateDbService.readCertTemplates(event.getOrElse("batchId", ""), event.getOrElse("courseId", ""))(metrics, cassandraUtil, jobConfig)
    map.keySet should contain("template_01_dev_001")
    metrics.get(s"${jobConfig.dbReadCount}") should be(1)
  }

  it should "throw exception invalid batchId readCertTemplates from courseBatch table" in intercept[Exception] {
    CertificateDbService.readCertTemplates("batch_00001", "do_11309999837886054415")(metrics, cassandraUtil, jobConfig)
  }

  it should "readUserIdsFromDb userEnrolments table without reIssue" in {
    metrics.reset(jobConfig.dbReadCount)
    val enrollmentCriteria = Map("status" -> 2).asInstanceOf[Map[String, AnyRef]].asJava
    val event = new Event(Map[String, Any](jobConfig.eData -> Map(jobConfig.userIds -> java.util.Arrays.asList("user001"), jobConfig.batchId -> "0131000245281587206", jobConfig.courseId -> "do_11309999837886054415").asJava).asJava)
    val templateName = "Course merit certificate"
    val list = CertificateDbService.readUserIdsFromDb(enrollmentCriteria, event, templateName)(metrics, cassandraUtil, jobConfig)
    list should be(empty)
    metrics.get(s"${jobConfig.dbReadCount}") should be(1)
  }

  it should "readUserIdsFromDb userEnrolments table with reIssue True" in {
    metrics.reset(jobConfig.dbReadCount)
    val enrollmentCriteria = Map("status" -> 2).asInstanceOf[Map[String, AnyRef]].asJava
    val event = new Event(Map[String, Any](jobConfig.eData -> Map[String, Any](jobConfig.userIds -> java.util.Arrays.asList("user001"), jobConfig.batchId -> "0131000245281587206", jobConfig.courseId -> "do_11309999837886054415", jobConfig.reIssue -> true.asInstanceOf[AnyRef]).asJava).asJava)
    val templateName = "Course merit certificate"
    val list = CertificateDbService.readUserIdsFromDb(enrollmentCriteria, event, templateName)(metrics, cassandraUtil, jobConfig)
    list should contain("user001")
    metrics.get(s"${jobConfig.dbReadCount}") should be(1)
  }

  it should "throw exception for invalid user readUserIdsFromDb" in intercept[Exception] {
    val enrollmentCriteria = Map("status" -> 2).asInstanceOf[Map[String, AnyRef]].asJava
    val event =  new Event(Map[String, Any](jobConfig.eData -> Map[String, Any](jobConfig.userIds -> List("user000001").asJava, jobConfig.batchId -> "0131000245281587206", jobConfig.courseId -> "do_11309999837886054415").asJava).asJava)
    val templateName = "Course merit certificate"
    CertificateDbService.readUserIdsFromDb(enrollmentCriteria, event, templateName)(metrics, cassandraUtil, jobConfig)
  }

  //check with multiple score for same user - need data
  it should "fetchAssessedUsersFromDB from assessment_aggregator table" in {
    metrics.reset(jobConfig.dbReadCount)
    val event = new Event(Map[String, Any](jobConfig.eData -> Map[String, Any](jobConfig.batchId -> "0131000245281587206", jobConfig.courseId -> "do_11309999837886054415", jobConfig.userIds -> java.util.Arrays.asList("user001")).asJava).asJava)
    val assessmentCriteria = Map(jobConfig.score -> 100.asInstanceOf[AnyRef]).asJava
    val list = CertificateDbService.fetchAssessedUsersFromDB(event, assessmentCriteria, event.userIds.asScala.toList)(metrics, cassandraUtil, jobConfig)
    list should contain("user001")
    metrics.get(s"${jobConfig.dbReadCount}") should be(1)
  }

  it should "throw exception for invalid batch for fetchAssessedUsersFromDB" in intercept[Exception] {
    val event = new Event(Map[String, Any](jobConfig.batchId -> "batch_0001", jobConfig.courseId -> "do_11309999837886054415").asJava)
    val assessmentCriteria = Map(jobConfig.score -> 3.asInstanceOf[AnyRef]).asJava
    CertificateDbService.fetchAssessedUsersFromDB(event, assessmentCriteria, null)(metrics, cassandraUtil, jobConfig)
  }

  // check date format for issuedDate
  it should "readUserCertificate from user_enrolments table" in {
    metrics.reset(jobConfig.dbReadCount)
    val edata = Map(jobConfig.userId -> "user001".asInstanceOf[AnyRef], jobConfig.batchId -> "0131000245281587206", jobConfig.courseId -> "do_11309999837886054415").asJava
    val map = CertificateDbService.readUserCertificate(edata)(metrics, cassandraUtil, jobConfig)
    map.keySet should contain allOf(jobConfig.issued_certificates, jobConfig.issuedDate)
    metrics.get(s"${jobConfig.dbReadCount}") should be(1)
  }

  it should "throw exception for invalid userId readUserCertificate from user_enrolments table" in intercept[Exception] {
    val edata = Map(jobConfig.userId -> "user0000001".asInstanceOf[AnyRef], jobConfig.batchId -> "0131000245281587206", jobConfig.courseId -> "do_11309999837886054415").asJava
    CertificateDbService.readUserCertificate(edata)(metrics, cassandraUtil, jobConfig)
  }

  private def testCassandraUtil(cassandraUtil: CassandraUtil): Unit = {
    cassandraUtil.reconnect()
  }
}
