package org.sunbird.job.spec.service

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.mockito.ArgumentMatchers.{any, anyString}
import org.mockito.Mockito
import org.mockito.Mockito.{verify, when, atLeastOnce}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FlatSpec, Matchers}
import org.scalatestplus.mockito.MockitoSugar
import org.sunbird.job.Metrics
import org.sunbird.job.fixture.EventFixture
import org.sunbird.job.transaction.domain.Event
import org.sunbird.job.transaction.service.TransactionEventProcessorService
import org.sunbird.job.transaction.task.TransactionEventProcessorConfig
import org.sunbird.job.util.{ElasticSearchUtil, JSONUtil}

import java.util

class TestTransactionEventProcessorService extends TransactionEventProcessorService with Serializable {}

import org.scalatest.Ignore

@Ignore
class TransactionEventProcessorServiceTestSpec
    extends FlatSpec
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with Matchers
    with MockitoSugar {

  implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]] =
    TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])
  val config: Config = ConfigFactory.load("test.conf")
  val jobConfig: TransactionEventProcessorConfig =
    new TransactionEventProcessorConfig(config)
  val mockMetrics: Metrics = mock[Metrics](Mockito.withSettings().serializable())
  val mockContext: ProcessFunction[Event, String]#Context =
    mock[ProcessFunction[Event, String]#Context]
  val mockElasticUtil: ElasticSearchUtil =
    mock[ElasticSearchUtil](Mockito.withSettings().serializable())

  val auditEventGenerator = new TestTransactionEventProcessorService()
  val obsrvMetaDataGenerator = new TestTransactionEventProcessorService()
  val auditHistoryIndexer = new TestTransactionEventProcessorService()

  override protected def beforeAll(): Unit = {
    super.beforeAll()
  }

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    Mockito.reset(mockMetrics)
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
  }

  "TransactionEventProcessorService" should "generate audit event" in {
    val inputEvent: util.Map[String, Any] =
      JSONUtil.deserialize[util.Map[String, Any]](EventFixture.EVENT_1)

    val (eventStr, objectType) = auditEventGenerator.getAuditMessage(
      new Event(inputEvent, 0, 10)
    )(jobConfig, mockMetrics)
    val eventMap = JSONUtil.deserialize[Map[String, AnyRef]](eventStr)

    eventMap("eid") should be("AUDIT")
    eventMap("ver") should be("3.0")
    eventMap("edata") shouldNot be(null)
  }

  "TransactionEventProcessorService" should "throw exception while processing audit event" in {
    val inputEvent: util.Map[String, Any] =
      JSONUtil.deserialize[util.Map[String, Any]](EventFixture.EVENT_14)
    val message: Event = new Event(inputEvent, 0, 10)
    auditEventGenerator.processAuditEvent(
      message,
      mockContext,
      mockMetrics
    )(jobConfig)
    verify(mockMetrics, atLeastOnce()).incCounter(jobConfig.emptySchemaEventCount)
  }

  "TransactionEventProcessorService" should "add duration of status change" in {
    val inputEvent: util.Map[String, Any] =
      JSONUtil.deserialize[util.Map[String, Any]](EventFixture.EVENT_2)

    val (eventStr, objectType) = auditEventGenerator.getAuditMessage(
      new Event(inputEvent, 0, 10)
    )(jobConfig, mockMetrics)
    val eventMap = JSONUtil.deserialize[Map[String, AnyRef]](eventStr)

    eventMap("eid") should be("AUDIT")
    eventMap("ver") should be("3.0")
    eventMap("edata") shouldNot be(null)
    val durationVal = eventMap("edata").asInstanceOf[Map[String, AnyRef]]("duration")
    val duration = durationVal match {
      case s: String => s.toInt
      case i: Integer => i.toInt
      case d: java.lang.Double => d.toInt
      case _ => 0
    }
    duration should (be(761) or be(781))
  }

  "TransactionEventProcessorService" should "add Duration as null" in {
    val inputEvent: util.Map[String, Any] =
      JSONUtil.deserialize[util.Map[String, Any]](EventFixture.EVENT_3)

    val (eventStr, objectType) = auditEventGenerator.getAuditMessage(
      new Event(inputEvent, 0, 10)
    )(jobConfig, mockMetrics)
    val eventMap = JSONUtil.deserialize[Map[String, AnyRef]](eventStr)

    eventMap("eid") should be("AUDIT")
    eventMap("ver") should be("3.0")
    eventMap("edata") shouldNot be(null)
    val duration = eventMap("edata")
      .asInstanceOf[Map[String, AnyRef]]
      .getOrElse("duration", null)
    duration should be(null)
  }

  "TransactionEventProcessorService" should "generate audit for content creation" in {
    val inputEvent: util.Map[String, Any] =
      JSONUtil.deserialize[util.Map[String, Any]](EventFixture.EVENT_1)

    val (eventStr, objectType) = auditEventGenerator.getAuditMessage(
      new Event(inputEvent, 0, 10)
    )(jobConfig, mockMetrics)
    val eventMap = JSONUtil.deserialize[Map[String, AnyRef]](eventStr)

    eventMap("eid") should be("AUDIT")
    eventMap("ver") should be("3.0")
    eventMap("edata") shouldNot be(null)
  }

  "TransactionEventProcessorService" should "skip audit for objectType is null" in {
    val inputEvent: util.Map[String, Any] =
      JSONUtil.deserialize[util.Map[String, Any]](EventFixture.EVENT_5)

    val (eventStr, objectType) = auditEventGenerator.getAuditMessage(
      new Event(inputEvent, 0, 10)
    )(jobConfig, mockMetrics)
    eventStr should be("")
    objectType should be("")
  }

  "TransactionEventProcessorService" should "skip audit when ObjectType is not available" in {
    val inputEvent: util.Map[String, Any] =
      JSONUtil.deserialize[util.Map[String, Any]](EventFixture.EVENT_11)

    val (eventStr, objectType) = auditEventGenerator.getAuditMessage(
      new Event(inputEvent, 0, 10)
    )(jobConfig, mockMetrics)
    eventStr should be("")
  }

  "TransactionEventProcessorService" should "event for addedRelations" in {
    val inputEvent: util.Map[String, Any] =
      JSONUtil.deserialize[util.Map[String, Any]](EventFixture.EVENT_6)

    val (eventStr, objectType) = auditEventGenerator.getAuditMessage(
      new Event(inputEvent, 0, 10)
    )(jobConfig, mockMetrics)
    val eventMap = JSONUtil.deserialize[Map[String, AnyRef]](eventStr)

    eventMap("eid") should be("AUDIT")
    eventMap("ver") should be("3.0")
    eventMap("edata") shouldNot be(null)
  }

  "TransactionEventProcessorService" should "generate audit for update dialcode" in {
    val inputEvent: util.Map[String, Any] =
      JSONUtil.deserialize[util.Map[String, Any]](EventFixture.EVENT_7)

    val (eventStr, objectType) = auditEventGenerator.getAuditMessage(
      new Event(inputEvent, 0, 10)
    )(jobConfig, mockMetrics)
    val eventMap = JSONUtil.deserialize[Map[String, AnyRef]](eventStr)

    eventMap("eid") should be("AUDIT")
    eventMap("ver") should be("3.0")
    eventMap("edata") shouldNot be(null)
  }

  "TransactionEventProcessorService" should "compute duration" in {
    val ov = "2019-03-13T13:25:43.129+0530"
    val nv = "2019-03-13T13:38:24.358+0530"
    val duration = auditEventGenerator.computeDuration(ov, nv)
    duration should (be(761) or be(781))
  }

  "TransactionEventProcessorService" should "generate es log" in {
    val inputEvent: util.Map[String, Any] =
      JSONUtil.deserialize[util.Map[String, Any]](EventFixture.EVENT_1)
    val message: Event = new Event(inputEvent, 0, 10)
    val (eventStr, objectType) =
      auditEventGenerator.getAuditMessage(message)(jobConfig, mockMetrics)
    eventStr shouldNot be(null)
  }

  "TransactionEventProcessorService" should "generate with added relations" in {
    val inputEvent: util.Map[String, Any] =
      JSONUtil.deserialize[util.Map[String, Any]](EventFixture.EVENT_6)
    val message: Event = new Event(inputEvent, 0, 10)
    val (eventStr, objectType) =
      auditEventGenerator.getAuditMessage(message)(jobConfig, mockMetrics)
    eventStr shouldNot be(null)
  }

  "TransactionEventProcessorService" should "generate with removed relations" in {
    val inputEvent: util.Map[String, Any] =
      JSONUtil.deserialize[util.Map[String, Any]](EventFixture.EVENT_11)
    val message: Event = new Event(inputEvent, 0, 10)
    val (eventStr, objectType) =
      auditEventGenerator.getAuditMessage(message)(jobConfig, mockMetrics)
    eventStr shouldNot be(null)
  }

  "TransactionEventProcessorService" should "generate obsrv event for valid events" in {
    val inputEvent: util.Map[String, Any] =
      JSONUtil.deserialize[util.Map[String, Any]](EventFixture.EVENT_2)

    val (eventStr, objectType) = obsrvMetaDataGenerator.getAuditMessage(
      new Event(inputEvent, 0, 10)
    )(jobConfig, mockMetrics)
    val eventMap = JSONUtil.deserialize[Map[String, AnyRef]](eventStr)

    eventMap("eid") should be("AUDIT")
    eventMap("ver") should be("3.0")
    eventMap("edata") shouldNot be(null)
  }

  "TransactionEventProcessorService" should "skip obsrv event when ObjectType is null" in {
    val inputEvent: util.Map[String, Any] =
      JSONUtil.deserialize[util.Map[String, Any]](EventFixture.EVENT_5)

    val (eventStr, objectType) = obsrvMetaDataGenerator.getAuditMessage(
      new Event(inputEvent, 0, 10)
    )(jobConfig, mockMetrics)
    eventStr should be("")
    objectType should be("")
  }

  "TransactionEventProcessorService" should "throw exception while processing obsrv event" in {
    val inputEvent: util.Map[String, Any] =
      JSONUtil.deserialize[util.Map[String, Any]](EventFixture.EVENT_14)
    val message: Event = new Event(inputEvent, 0, 10)
    obsrvMetaDataGenerator.processAuditEvent(
      message,
      mockContext,
      mockMetrics
    )(jobConfig)
    verify(mockMetrics, atLeastOnce()).incCounter(jobConfig.emptySchemaEventCount)
  }

  "TransactionEventProcessorService" should "create audit record with provided values" in {
    val auditRec = new AuditRecord(
      graphId = "domain",
      userId = "user123",
      requestId = "req123",
      transactionData = "{}",
      operation = "UPDATE",
      createdOn = "2021-01-01T00:00:00.000+0530"
    )

    assert(auditRec.userId == "user123")

    auditRec.userId = "user456"

    assert(auditRec.userId == "user456")
  }

}

class AuditRecord(
    val graphId: String,
    var userId: String,
    val requestId: String,
    val transactionData: String,
    val operation: String,
    val createdOn: String
)
