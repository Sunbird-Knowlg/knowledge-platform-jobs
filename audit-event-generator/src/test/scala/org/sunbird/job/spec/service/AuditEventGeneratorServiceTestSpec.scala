package org.sunbird.job.spec.service

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.sunbird.job.domain.Event
import org.sunbird.job.fixture.EventFixture
import org.sunbird.job.functions.AuditEventGenerator
import org.sunbird.job.task.AuditEventGeneratorConfig
import org.sunbird.job.util.JSONUtil
import org.sunbird.spec.BaseTestSpec

import java.util

class AuditEventGeneratorServiceTestSpec extends BaseTestSpec {
  implicit val mapTypeInfo: TypeInformation[java.util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[java.util.Map[String, AnyRef]])
  implicit val strTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

  val config: Config = ConfigFactory.load("test.conf")
  lazy val jobConfig: AuditEventGeneratorConfig = new AuditEventGeneratorConfig(config)
  lazy val auditEventGenerator:AuditEventGenerator = new AuditEventGenerator(jobConfig)

  override protected def beforeAll(): Unit = {
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
  }

  "AuditEventGeneratorService" should "generate audit event" in {
    val inputEvent:util.Map[String, Any] = JSONUtil.deserialize[util.Map[String, Any]](EventFixture.EVENT_1)

    val eventStr = auditEventGenerator.getAuditMessage(new Event(inputEvent))(jobConfig)
    val eventMap = JSONUtil.deserialize[Map[String, AnyRef]](eventStr)

    eventMap("eid") should be("AUDIT")
    eventMap("ver") should be("3.0")
    eventMap("edata") shouldNot be(null)
  }

  "AuditEventGeneratorService" should "add duration of status change" in {
    val inputEvent:util.Map[String, Any] = JSONUtil.deserialize[util.Map[String, Any]](EventFixture.EVENT_2)

    val eventStr = auditEventGenerator.getAuditMessage(new Event(inputEvent))(jobConfig);
    val eventMap = JSONUtil.deserialize[Map[String, AnyRef]](eventStr)

    eventMap("eid") should be("AUDIT")
    eventMap("ver") should be("3.0")
    eventMap("edata") shouldNot be(null)
    val duration = eventMap("edata").asInstanceOf[Map[String, AnyRef]]("duration").asInstanceOf[Int]
    duration should be(761)
  }

  "AuditEventGeneratorService" should "add Duration as null" in {
    val inputEvent:util.Map[String, Any] = JSONUtil.deserialize[util.Map[String, Any]](EventFixture.EVENT_3)

    val eventStr = auditEventGenerator.getAuditMessage(new Event(inputEvent))(jobConfig);
    val eventMap = JSONUtil.deserialize[Map[String, AnyRef]](eventStr)

    eventMap("eid") should be("AUDIT")
    eventMap("ver") should be("3.0")
    eventMap("edata") shouldNot be(null)
    val duration = eventMap("edata").asInstanceOf[Map[String, AnyRef]].getOrElse("duration", null)
    duration should be(null)
  }

  "AuditEventGeneratorService" should "generate audit for content creation" in {
    val inputEvent:util.Map[String, Any] = JSONUtil.deserialize[util.Map[String, Any]](EventFixture.EVENT_4)

    val eventStr = auditEventGenerator.getAuditMessage(new Event(inputEvent))(jobConfig);
    val eventMap = JSONUtil.deserialize[Map[String, AnyRef]](eventStr)

    eventMap("eid") should be("AUDIT")
    eventMap("ver") should be("3.0")
    eventMap("edata") shouldNot be(null)
    val duration = eventMap("edata").asInstanceOf[Map[String, AnyRef]].getOrElse("duration", null)
    duration should be(null)
  }

  "AuditEventGeneratorService" should "skip audit for props is null" in {
    val inputEvent:util.Map[String, Any] = JSONUtil.deserialize[util.Map[String, Any]](EventFixture.EVENT_5)

    val eventStr = auditEventGenerator.getAuditMessage(new Event(inputEvent))(jobConfig);

    eventStr should be("{\"object\": {\"type\":null}}")
  }

  "AuditEventGeneratorService" should "compute duration" in {
    val ov = "2019-03-13T13:25:43.129+0530"
    val nv = "2019-03-13T13:38:24.358+0530"
    val duration = auditEventGenerator.computeDuration(ov, nv)
    duration should be(761)
  }
}
