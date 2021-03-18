package org.sunbird.job.spec.service

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.sunbird.job.domain.Event
import org.sunbird.job.fixture.EventFixture
import org.sunbird.job.functions.AutoCreatorV2
import org.sunbird.job.task.AutoCreatorV2Config
import org.sunbird.job.util.JSONUtil
import org.sunbird.spec.BaseTestSpec

import java.util

class AutoCreatorV2ServiceTestSpec extends BaseTestSpec {
  implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])
  implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

  val config: Config = ConfigFactory.load("test.conf")
  lazy val jobConfig: AutoCreatorV2Config = new AutoCreatorV2Config(config)
  lazy val autoCreatorV2: AutoCreatorV2 = new AutoCreatorV2(jobConfig)

  override protected def beforeAll(): Unit = {
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
  }

  "AutoCreatorV2Service" should "generate event" in {
    val inputEvent:util.Map[String, Any] = JSONUtil.deserialize[util.Map[String, Any]](EventFixture.EVENT_1)
  }
}
