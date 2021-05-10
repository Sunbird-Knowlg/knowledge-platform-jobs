package org.sunbird.job.spec.service

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.mockito.Mockito
import org.sunbird.job.audithistory.domain.{AuditHistoryRecord, Event}
import org.sunbird.job.fixture.EventFixture
import org.sunbird.job.functions.AuditHistoryIndexer
import org.sunbird.job.task.AuditHistoryIndexerConfig
import org.sunbird.job.util.{ElasticSearchUtil, JSONUtil}
import org.sunbird.spec.BaseTestSpec

import java.util

class AuditHistoryIndexerServiceTestSpec extends BaseTestSpec {
  implicit val mapTypeInfo: TypeInformation[util.Map[String, Any]] = TypeExtractor.getForClass(classOf[util.Map[String, Any]])
  implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

  val config: Config = ConfigFactory.load("test.conf")
  lazy val jobConfig: AuditHistoryIndexerConfig = new AuditHistoryIndexerConfig(config)
  val mockElasticUtil:ElasticSearchUtil = mock[ElasticSearchUtil](Mockito.withSettings().serializable())
  lazy val auditHistoryIndexer: AuditHistoryIndexer = new AuditHistoryIndexer(jobConfig, mockElasticUtil)

  override protected def beforeAll(): Unit = {
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
  }

  "AuditHistoryIndexerService" should "generate es log" in {
    val inputEvent:Event = new Event(JSONUtil.deserialize[util.Map[String, Any]](EventFixture.EVENT_1),0, 10)

    val auditHistoryRec:AuditHistoryRecord = auditHistoryIndexer.getAuditHistory(inputEvent);

    auditHistoryRec.objectId should be(inputEvent.nodeUniqueId)
    auditHistoryRec.objectType should be(inputEvent.objectType)
    auditHistoryRec.logRecord should be("""{"properties":{"mediaType":{"nv":"content"},"name":{"nv":"Untitled Resource"},"createdOn":{"nv":"2018-02-13T16:01:18.947+0530"},"channel":{"nv":"in.ekstep"},"lastUpdatedOn":{"nv":"2018-02-13T16:01:18.947+0530"},"IL_FUNC_OBJECT_TYPE":{"nv":"Content"},"resourceType":{"nv":"Story"},"compatibilityLevel":{"nv":1.0},"audience":{"nv":["Learner"]},"os":{"nv":["All"]},"IL_SYS_NODE_TYPE":{"nv":"DATA_NODE"},"framework":{"nv":"NCF"},"versionKey":{"nv":"1518517878947"},"mimeType":{"nv":"application/pdf"},"code":{"nv":"test_code"},"contentType":{"nv":"Story"},"language":{"nv":["English"]},"status":{"nv":"Draft"},"keywords":{"nv":["colors","games"]},"idealScreenSize":{"nv":"normal"},"contentEncoding":{"nv":"identity"},"osId":{"nv":"org.ekstep.quiz.app"},"IL_UNIQUE_ID":{"nv":"do_11243969846440755213"},"contentDisposition":{"nv":"inline"},"visibility":{"nv":"Default"},"idealScreenDensity":{"nv":"hdpi"}}}""")
  }

  "AuditHistoryIndexerService" should "generate with added relations" in {
    val inputEvent:Event = new Event(JSONUtil.deserialize[util.Map[String, Any]](EventFixture.EVENT_2), 0, 11)

    val auditHistoryRec:AuditHistoryRecord = auditHistoryIndexer.getAuditHistory(inputEvent);

    auditHistoryRec.objectId should be(inputEvent.nodeUniqueId)
    auditHistoryRec.objectType should be(inputEvent.objectType)
    auditHistoryRec.logRecord should be("""{"addedRelations":[{"label":"Test unit 11","rel":"hasSequenceMember","dir":"IN","id":"do_1123032073439723521148","type":"Content"}],"removedRelations":[],"properties":{"name":{"nv":"","ov":""}}}""")
  }

  "AuditHistoryIndexerService" should "generate with removed relations" in {
    val inputEvent:Event = new Event(JSONUtil.deserialize[util.Map[String, Any]](EventFixture.EVENT_3),0, 12)

    val auditHistoryRec:AuditHistoryRecord = auditHistoryIndexer.getAuditHistory(inputEvent);

    auditHistoryRec.objectId should be(inputEvent.nodeUniqueId)
    auditHistoryRec.objectType should be(inputEvent.objectType)
    auditHistoryRec.logRecord should be("""{"addedTags":[],"addedRelations":[],"properties":{},"removedRelations":[{"label":"qq\n","rel":"associatedTo","dir":"OUT","id":"do_113198273083662336127","relMetadata":{},"type":"AssessmentItem"}],"removedTags":[]}""")
  }
}
