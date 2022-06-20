package org.sunbird.job.dialcodecontextupdater.spec.domain

import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.{FlatSpec, Matchers}
import org.scalatestplus.mockito.MockitoSugar
import org.sunbird.job.dialcodecontextupdater.domain.Event
import org.sunbird.job.dialcodecontextupdater.task.DialcodeContextUpdaterConfig
import org.sunbird.job.util.JSONUtil

class EventSpec extends FlatSpec with Matchers with MockitoSugar {

  val config: Config = ConfigFactory.load("test.conf").withFallback(ConfigFactory.systemEnvironment())
  val jobConfig: DialcodeContextUpdaterConfig = new DialcodeContextUpdaterConfig(config)

  "isValid" should "return true for a valid event" in {
    val sunbirdEvent = """{"eid":"BE_JOB_REQUEST","ets":1648720639981,"mid":"LP.1648720639981.d6b1d8c8-7a4a-483a-b83a-b752bede648c","actor":{"id":"DIALcodecontextupdateJob","type":"System"},"context":{"pdata":{"ver":"1.0","id":"org.sunbird.platform"},"channel":"01269878797503692810","env":"dev"},"object":{"ver":"1.0","id":"0117CH01"},"edata":{"action":"dialcode-context-update","iteration":1,"dialcode":"0117CH01","identifier":"d0_1234","traceId":"2342345345"}}"""
    val event = new Event(JSONUtil.deserialize[java.util.Map[String, Any]](sunbirdEvent),0,1)

    assert(event.isValid())
  }
}