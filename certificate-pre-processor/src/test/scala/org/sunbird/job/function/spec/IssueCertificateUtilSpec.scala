package org.sunbird.job.function.spec

import java.util

import com.google.gson.Gson
import com.typesafe.config.{Config, ConfigFactory}
import org.sunbird.job.domain.CertTemplate
import org.sunbird.job.fixture.EventFixture
import org.sunbird.job.functions.IssueCertificateUtil
import org.sunbird.job.task.CertificatePreProcessorConfig
import org.sunbird.spec.BaseTestSpec

class IssueCertificateUtilSpec extends BaseTestSpec{

  val gson = new Gson()
  val config: Config = ConfigFactory.load("test.conf")
  val jobConfig: CertificatePreProcessorConfig = new CertificatePreProcessorConfig(config)

  it should "prepareTemplate" in {
    val template = gson.fromJson(EventFixture.USER_UTIL_TEMPLATE, new util.LinkedHashMap[String, AnyRef]().getClass).asInstanceOf[util.Map[String, AnyRef]]
    val certTemplate = IssueCertificateUtil.prepareTemplate(template)(jobConfig)
    certTemplate.isInstanceOf[CertTemplate]
  }
}
