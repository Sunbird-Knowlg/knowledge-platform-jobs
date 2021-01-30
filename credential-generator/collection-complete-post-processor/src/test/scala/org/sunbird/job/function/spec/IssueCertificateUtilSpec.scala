package org.sunbird.job.function.spec

import com.google.gson.Gson
import com.typesafe.config.{Config, ConfigFactory}
import org.sunbird.job.domain.CertTemplate
import org.sunbird.job.fixture.EventFixture
import org.sunbird.job.functions.IssueCertificateUtil
import org.sunbird.job.task.CollectionCompletePostProcessorConfig
import org.sunbird.spec.BaseTestSpec
import scala.collection.JavaConverters._

class IssueCertificateUtilSpec extends BaseTestSpec {

  val gson = new Gson()
  val config: Config = ConfigFactory.load("test.conf")
  val jobConfig: CollectionCompletePostProcessorConfig = new CollectionCompletePostProcessorConfig(config)

  it should "prepareTemplate" in {
    val template = gson.fromJson(EventFixture.USER_UTIL_TEMPLATE, classOf[java.util.Map[String, AnyRef]]).asScala.toMap
    val certTemplate = IssueCertificateUtil.prepareTemplate(template)(jobConfig)
    certTemplate.isInstanceOf[CertTemplate]
  }

}
