package org.sunbird.job.autocreatorv2.spec.service

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.mockito.Mockito
import org.sunbird.job.autocreatorv2.fixture.EventFixture
import org.sunbird.job.autocreatorv2.functions.AutoCreatorFunction
import org.sunbird.job.task.AutoCreatorV2Config
import org.sunbird.job.util.{HttpUtil, JSONUtil}
import org.sunbird.spec.BaseTestSpec

import java.util

class AutoCreatorFunctionSpec extends BaseTestSpec {
  implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])
  implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

  val config: Config = ConfigFactory.load("test.conf")
  val mockHttpUtil = mock[HttpUtil](Mockito.withSettings().serializable())
  lazy val jobConfig: AutoCreatorV2Config = new AutoCreatorV2Config(config)
  lazy val autoCreatorV2: AutoCreatorFunction = new AutoCreatorFunction(jobConfig, mockHttpUtil)

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
