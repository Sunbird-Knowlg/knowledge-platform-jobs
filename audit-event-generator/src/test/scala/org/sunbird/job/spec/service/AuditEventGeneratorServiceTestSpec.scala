package org.sunbird.job.spec.service

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.mockito.Mockito
import org.sunbird.job.Metrics
import org.sunbird.job.fixture.EventFixture
import org.sunbird.job.service.AuditEventGeneratorService
import org.sunbird.job.task.AuditEventGeneratorConfig
import org.sunbird.job.util.JSONUtil
import org.sunbird.spec.BaseTestSpec

import java.util

class AuditEventGeneratorServiceTestSpec extends BaseTestSpec {
  val mockContext:ProcessFunction[util.Map[String, AnyRef], String]#Context = mock[ProcessFunction[util.Map[String, AnyRef], String]#Context](Mockito.withSettings().serializable())
  val mockMetrics = mock[Metrics](Mockito.withSettings().serializable())
  val mockConfig = mock[AuditEventGeneratorConfig]
  val config: Config = ConfigFactory.load("test.conf")
  lazy val jobConfig: AuditEventGeneratorConfig = new AuditEventGeneratorConfig(config)
  val auditEventService:AuditEventGeneratorService = new AuditEventGeneratorService()(jobConfig)


  override protected def beforeAll(): Unit = {
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
  }

  "AuditEventGeneratorService" should "generate audit event" in {
    val inputEvent:Map[String, AnyRef] = JSONUtil.deserialize[Map[String, AnyRef]](EventFixture.EVENT_1)

    val eventStr = auditEventService.getAuditMessage(inputEvent);
    val eventMap = JSONUtil.deserialize[Map[String, AnyRef]](eventStr)

    eventMap("eid") should be("AUDIT")
    eventMap("ver") should be("3.0")
    eventMap("edata") shouldNot be(null)
  }

  "AuditEventGeneratorService" should "add duration of status change" in {
    val inputEvent:Map[String, AnyRef] = JSONUtil.deserialize[Map[String, AnyRef]](EventFixture.EVENT_2)

    val eventStr = auditEventService.getAuditMessage(inputEvent);
    val eventMap = JSONUtil.deserialize[Map[String, AnyRef]](eventStr)

    eventMap("eid") should be("AUDIT")
    eventMap("ver") should be("3.0")
    eventMap("edata") shouldNot be(null)
    val duration = eventMap("edata").asInstanceOf[Map[String, AnyRef]]("duration").asInstanceOf[Int]
    duration should be(761)
  }

  "AuditEventGeneratorService" should "add Duration as null" in {
    val inputEvent:Map[String, AnyRef] = JSONUtil.deserialize[Map[String, AnyRef]](EventFixture.EVENT_3)

    val eventStr = auditEventService.getAuditMessage(inputEvent);
    val eventMap = JSONUtil.deserialize[Map[String, AnyRef]](eventStr)

    eventMap("eid") should be("AUDIT")
    eventMap("ver") should be("3.0")
    eventMap("edata") shouldNot be(null)
    val duration = eventMap("edata").asInstanceOf[Map[String, AnyRef]].getOrElse("duration", null)
    duration should be(null)
  }

  "AuditEventGeneratorService" should "generate audit for content creation" in {
    val inputEvent:Map[String, AnyRef] = JSONUtil.deserialize[Map[String, AnyRef]](EventFixture.EVENT_4)

    val eventStr = auditEventService.getAuditMessage(inputEvent);
    val eventMap = JSONUtil.deserialize[Map[String, AnyRef]](eventStr)

    eventMap("eid") should be("AUDIT")
    eventMap("ver") should be("3.0")
    eventMap("edata") shouldNot be(null)
    val duration = eventMap("edata").asInstanceOf[Map[String, AnyRef]].getOrElse("duration", null)
    duration should be(null)
  }

  "AuditEventGeneratorService" should "skip audit for props is null" in {
    val inputEvent:Map[String, AnyRef] = JSONUtil.deserialize[Map[String, AnyRef]](EventFixture.EVENT_5)

    val eventStr = auditEventService.getAuditMessage(inputEvent)

    eventStr should be("{\"object\": {\"type\":null}}")
  }

  "AuditEventGeneratorService" should "compute duration" in {
    val ov = "2019-03-13T13:25:43.129+0530"
    val nv = "2019-03-13T13:38:24.358+0530"
    val duration = auditEventService.computeDuration(ov, nv)
    duration should be(761)
  }
}
