package org.sunbird.job.spec.service

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.sunbird.job.domain.Event
import org.sunbird.job.fixture.EventFixture
import org.sunbird.job.functions.AuditHistoryIndexer
import org.sunbird.job.task.AuditHistoryIndexerConfig
import org.sunbird.job.util.JSONUtil
import org.sunbird.spec.BaseTestSpec

import java.util

class AuditHistoryIndexerServiceTestSpec extends BaseTestSpec {
  implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])
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

  "AuditHistoryIndexerService" should "generate event" in {
    val inputEvent:util.Map[String, Any] = JSONUtil.deserialize[util.Map[String, Any]](EventFixture.EVENT_1)
  }
}
