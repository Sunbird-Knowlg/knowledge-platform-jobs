package org.sunbird.job.spec.service

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.sunbird.job.audithistory.domain.{AuditHistoryRecord, Event}
import org.sunbird.job.fixture.EventFixture
import org.sunbird.job.functions.AuditHistoryIndexer
import org.sunbird.job.task.AuditHistoryIndexerConfig
import org.sunbird.job.util.JSONUtil
import org.sunbird.spec.BaseTestSpec

import java.util

class AuditHistoryIndexerServiceTestSpec extends BaseTestSpec {
  implicit val mapTypeInfo: TypeInformation[util.Map[String, Any]] = TypeExtractor.getForClass(classOf[util.Map[String, Any]])
  implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

  val config: Config = ConfigFactory.load("test.conf")
  lazy val jobConfig: AuditHistoryIndexerConfig = new AuditHistoryIndexerConfig(config)
  lazy val auditHistoryIndexer: AuditHistoryIndexer = new AuditHistoryIndexer(jobConfig)

  override protected def beforeAll(): Unit = {
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
  }

  "AuditHistoryIndexerService" should "generate es log" in {
    val inputEvent:Event = new Event(JSONUtil.deserialize[util.Map[String, Any]](EventFixture.EVENT_1))

    val auditHistoryRec:AuditHistoryRecord = auditHistoryIndexer.getAuditHistory(inputEvent);

    auditHistoryRec.objectId should be(inputEvent.nodeUniqueId)
    auditHistoryRec.objectType should be(inputEvent.objectType)
    auditHistoryRec.logRecord should be("""{"properties":{"mediaType":{"nv":"content"},"name":{"nv":"Untitled Resource"},"createdOn":{"nv":"2018-02-13T16:01:18.947+0530"},"channel":{"nv":"in.ekstep"},"lastUpdatedOn":{"nv":"2018-02-13T16:01:18.947+0530"},"IL_FUNC_OBJECT_TYPE":{"nv":"Content"},"resourceType":{"nv":"Story"},"compatibilityLevel":{"nv":1.0},"audience":{"nv":["Learner"]},"os":{"nv":["All"]},"IL_SYS_NODE_TYPE":{"nv":"DATA_NODE"},"framework":{"nv":"NCF"},"versionKey":{"nv":"1518517878947"},"mimeType":{"nv":"application/pdf"},"code":{"nv":"test_code"},"contentType":{"nv":"Story"},"language":{"nv":["English"]},"status":{"nv":"Draft"},"keywords":{"nv":["colors","games"]},"idealScreenSize":{"nv":"normal"},"contentEncoding":{"nv":"identity"},"osId":{"nv":"org.ekstep.quiz.app"},"IL_UNIQUE_ID":{"nv":"do_11243969846440755213"},"contentDisposition":{"nv":"inline"},"visibility":{"nv":"Default"},"idealScreenDensity":{"nv":"hdpi"}}}""")

    auditHistoryRec.summary should be("""{"properties":{"count":26,"fields":["mediaType","name","createdOn","channel","lastUpdatedOn","IL_FUNC_OBJECT_TYPE","resourceType","compatibilityLevel","audience","os","IL_SYS_NODE_TYPE","framework","versionKey","mimeType","code","contentType","language","status","keywords","idealScreenSize","contentEncoding","osId","IL_UNIQUE_ID","contentDisposition","visibility","idealScreenDensity"]}}""")
  }

  "AuditHistoryIndexerService" should "generate with added relations" in {
    val inputEvent:Event = new Event(JSONUtil.deserialize[util.Map[String, Any]](EventFixture.EVENT_2))

    val auditHistoryRec:AuditHistoryRecord = auditHistoryIndexer.getAuditHistory(inputEvent);

    auditHistoryRec.objectId should be(inputEvent.nodeUniqueId)
    auditHistoryRec.objectType should be(inputEvent.objectType)
    auditHistoryRec.logRecord should be("""{"addedRelations":[{"label":"Test unit 11","rel":"hasSequenceMember","dir":"IN","id":"do_1123032073439723521148","type":"Content"}],"removedRelations":[],"properties":{"name":{"nv":"","ov":""}}}""")

    auditHistoryRec.summary should be("""{"relations":{"addedRelations":1,"removedRelations":0},"properties":{"count":1,"fields":["name"]}}""")
  }

  "AuditHistoryIndexerService" should "generate with removed relations" in {
    val inputEvent:Event = new Event(JSONUtil.deserialize[util.Map[String, Any]](EventFixture.EVENT_3))

    val auditHistoryRec:AuditHistoryRecord = auditHistoryIndexer.getAuditHistory(inputEvent);

    auditHistoryRec.objectId should be(inputEvent.nodeUniqueId)
    auditHistoryRec.objectType should be(inputEvent.objectType)
    auditHistoryRec.logRecord should be("""{"addedTags":[],"addedRelations":[],"properties":{},"removedRelations":[{"label":"qq\n","rel":"associatedTo","dir":"OUT","id":"do_113198273083662336127","relMetadata":{},"type":"AssessmentItem"}],"removedTags":[]}""")

    auditHistoryRec.summary should be("""{"relations":{"addedRelations":0,"removedRelations":1},"properties":{"count":0,"fields":[]},"tags":{"removedTags":0,"addedTags":0}}""")
  }
}
