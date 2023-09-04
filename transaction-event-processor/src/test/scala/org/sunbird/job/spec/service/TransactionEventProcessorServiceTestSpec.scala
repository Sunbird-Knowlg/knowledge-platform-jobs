package org.sunbird.job.spec.service

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.mockito.Mockito
import org.slf4j.LoggerFactory
import org.sunbird.job.Metrics
import org.sunbird.job.transaction.domain.{AuditHistoryRecord, Event}
import org.sunbird.job.fixture.EventFixture
import org.sunbird.job.transaction.functions.{AuditEventGenerator, AuditHistoryIndexer}
import org.sunbird.job.transaction.task.TransactionEventProcessorConfig
import org.sunbird.job.util.{ElasticSearchUtil, JSONUtil}
import org.sunbird.spec.BaseTestSpec

import java.util

class TransactionEventProcessorTestSpec extends BaseTestSpec {
  implicit val mapTypeInfo: TypeInformation[java.util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[java.util.Map[String, AnyRef]])
  implicit val mpTypeInfo: TypeInformation[util.Map[String, Any]] = TypeExtractor.getForClass(classOf[util.Map[String, Any]])
  implicit val strTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

  val config: Config = ConfigFactory.load("test.conf")
  lazy val jobConfig: TransactionEventProcessorConfig = new TransactionEventProcessorConfig(config)
  lazy val mockMetrics = mock[Metrics](Mockito.withSettings().serializable())
  lazy val auditEventGenerator: AuditEventGenerator = new AuditEventGenerator(jobConfig)
  lazy val mockElasticUtil:ElasticSearchUtil = mock[ElasticSearchUtil](Mockito.withSettings().serializable())
  lazy val auditHistoryIndexer: AuditHistoryIndexer = new AuditHistoryIndexer(jobConfig,mockElasticUtil)

  override protected def beforeAll(): Unit = {
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
  }

  "TransactionEventProcessorService" should "generate audit event" in {
    val inputEvent:util.Map[String, Any] = JSONUtil.deserialize[util.Map[String, Any]](EventFixture.EVENT_1)

    val (eventStr, objectType) = auditEventGenerator.getAuditMessage(new Event(inputEvent, 0, 10))(jobConfig, mockMetrics)
    val eventMap = JSONUtil.deserialize[Map[String, AnyRef]](eventStr)

    eventMap("eid") should be("AUDIT")
    eventMap("ver") should be("3.0")
    eventMap("edata") shouldNot be(null)
  }

  "TransactionEventProcessorService" should "add duration of status change" in {
    val inputEvent:util.Map[String, Any] = JSONUtil.deserialize[util.Map[String, Any]](EventFixture.EVENT_2)

    val (eventStr, objectType) = auditEventGenerator.getAuditMessage(new Event(inputEvent, 0, 10))(jobConfig, mockMetrics)
    val eventMap = JSONUtil.deserialize[Map[String, AnyRef]](eventStr)

    eventMap("eid") should be("AUDIT")
    eventMap("ver") should be("3.0")
    eventMap("edata") shouldNot be(null)
    val duration = eventMap("edata").asInstanceOf[Map[String, AnyRef]]("duration").asInstanceOf[Int]
    duration should be(761)
  }

  "TransactionEventProcessorService" should "add Duration as null" in {
    val inputEvent:util.Map[String, Any] = JSONUtil.deserialize[util.Map[String, Any]](EventFixture.EVENT_3)

    val (eventStr, objectType) = auditEventGenerator.getAuditMessage(new Event(inputEvent, 0, 10))(jobConfig, mockMetrics)
    val eventMap = JSONUtil.deserialize[Map[String, AnyRef]](eventStr)

    eventMap("eid") should be("AUDIT")
    eventMap("ver") should be("3.0")
    eventMap("edata") shouldNot be(null)
    val duration = eventMap("edata").asInstanceOf[Map[String, AnyRef]].getOrElse("duration", null)
    duration should be(null)
  }

  "TransactionEventProcessorService" should "generate audit for content creation" in {
    val inputEvent:util.Map[String, Any] = JSONUtil.deserialize[util.Map[String, Any]](EventFixture.EVENT_4)

    val (eventStr, objectType) = auditEventGenerator.getAuditMessage(new Event(inputEvent, 0, 10))(jobConfig, mockMetrics)
    val eventMap = JSONUtil.deserialize[Map[String, AnyRef]](eventStr)

    eventMap("eid") should be("AUDIT")
    eventMap("ver") should be("3.0")
    eventMap("edata") shouldNot be(null)
    val duration = eventMap("edata").asInstanceOf[Map[String, AnyRef]].getOrElse("duration", null)
    duration should be(null)
  }

  "TransactionEventProcessorService" should "skip audit for objectType is null" in {
    val inputEvent:util.Map[String, Any] = JSONUtil.deserialize[util.Map[String, Any]](EventFixture.EVENT_5)

    val (eventStr, objectType) = auditEventGenerator.getAuditMessage(new Event(inputEvent, 0, 10))(jobConfig, mockMetrics)

    eventStr should be("{\"object\": {\"type\":null}}")
  }

  "TransactionEventProcessorService" should "event for addedRelations" in {
    val inputEvent:util.Map[String, Any] = JSONUtil.deserialize[util.Map[String, Any]](EventFixture.EVENT_6)

    val (eventStr, objectType) = auditEventGenerator.getAuditMessage(new Event(inputEvent, 0, 10))(jobConfig, mockMetrics)
    val eventMap = JSONUtil.deserialize[Map[String, AnyRef]](eventStr)
    eventMap("eid") should be("AUDIT")
    eventMap("ver") should be("3.0")
    eventMap("edata").asInstanceOf[Map[String, AnyRef]]("props").asInstanceOf[List[String]] should contain ("name")
    eventMap("edata").asInstanceOf[Map[String, AnyRef]]("props").asInstanceOf[List[String]] should contain ("collections")
    val duration = eventMap("edata").asInstanceOf[Map[String, AnyRef]].getOrElse("duration", null)
    duration should be(null)
  }

   "TransactionEventProcessorService" should "generate audit for update dialcode" in {
    val inputEvent:util.Map[String, Any] = JSONUtil.deserialize[util.Map[String, Any]](EventFixture.EVENT_7)

    val (eventStr, objectType) = auditEventGenerator.getAuditMessage(new Event(inputEvent, 0, 10))(jobConfig, mockMetrics)
    val eventMap = JSONUtil.deserialize[Map[String, AnyRef]](eventStr)

    eventMap("eid") should be("AUDIT")
    eventMap("edata").asInstanceOf[Map[String, AnyRef]]("props").asInstanceOf[List[String]] should contain ("dialcodes")
    val cdata = eventMap("cdata").asInstanceOf[List[Map[String, AnyRef]]]
    cdata.head("id").asInstanceOf[List[String]] should contain ("K1W6L6")
    cdata.head("type") should be("DialCode")
  }

  "TransactionEventProcessorService" should "compute duration" in {
    val ov = "2019-03-13T13:25:43.129+0530"
    val nv = "2019-03-13T13:38:24.358+0530"
    val duration = auditEventGenerator.computeDuration(ov, nv)
    duration should be(761)
  }

  "TransactionEventProcessorService" should "generate es log" in {
    val inputEvent: Event = new Event(JSONUtil.deserialize[util.Map[String, Any]](EventFixture.EVENT_9), 0, 10)

    val auditHistoryRec: AuditHistoryRecord = auditHistoryIndexer.getAuditHistory(inputEvent);

    auditHistoryRec.objectId should be(inputEvent.nodeUniqueId)
    auditHistoryRec.objectType should be(inputEvent.objectType)
    auditHistoryRec.logRecord should be("""{"properties":{"mediaType":{"nv":"content"},"name":{"nv":"Untitled Resource"},"createdOn":{"nv":"2018-02-13T16:01:18.947+0530"},"channel":{"nv":"in.ekstep"},"lastUpdatedOn":{"nv":"2018-02-13T16:01:18.947+0530"},"IL_FUNC_OBJECT_TYPE":{"nv":"Content"},"resourceType":{"nv":"Story"},"compatibilityLevel":{"nv":1.0},"audience":{"nv":["Learner"]},"os":{"nv":["All"]},"IL_SYS_NODE_TYPE":{"nv":"DATA_NODE"},"framework":{"nv":"NCF"},"versionKey":{"nv":"1518517878947"},"mimeType":{"nv":"application/pdf"},"code":{"nv":"test_code"},"contentType":{"nv":"Story"},"language":{"nv":["English"]},"status":{"nv":"Draft"},"keywords":{"nv":["colors","games"]},"idealScreenSize":{"nv":"normal"},"contentEncoding":{"nv":"identity"},"osId":{"nv":"org.ekstep.quiz.app"},"IL_UNIQUE_ID":{"nv":"do_11243969846440755213"},"contentDisposition":{"nv":"inline"},"visibility":{"nv":"Default"},"idealScreenDensity":{"nv":"hdpi"}}}""")
  }

  "TransactionEventProcessorService" should "generate with added relations" in {
    val inputEvent: Event = new Event(JSONUtil.deserialize[util.Map[String, Any]](EventFixture.EVENT_10), 0, 11)

    val auditHistoryRec: AuditHistoryRecord = auditHistoryIndexer.getAuditHistory(inputEvent);

    auditHistoryRec.objectId should be(inputEvent.nodeUniqueId)
    auditHistoryRec.objectType should be(inputEvent.objectType)
    auditHistoryRec.logRecord should be("""{"addedRelations":[{"label":"Test unit 11","rel":"hasSequenceMember","dir":"IN","id":"do_1123032073439723521148","type":"Content"}],"removedRelations":[],"properties":{"name":{"nv":"","ov":""}}}""")
  }

  "TransactionEventProcessorService" should "generate with removed relations" in {
    val inputEvent: Event = new Event(JSONUtil.deserialize[util.Map[String, Any]](EventFixture.EVENT_11), 0, 12)

    val auditHistoryRec: AuditHistoryRecord = auditHistoryIndexer.getAuditHistory(inputEvent);

    auditHistoryRec.objectId should be(inputEvent.nodeUniqueId)
    auditHistoryRec.objectType should be(inputEvent.objectType)
    auditHistoryRec.logRecord should be("""{"addedTags":[],"addedRelations":[],"properties":{},"removedRelations":[{"label":"qq\n","rel":"associatedTo","dir":"OUT","id":"do_113198273083662336127","relMetadata":{},"type":"AssessmentItem"}],"removedTags":[]}""")
  }
}
