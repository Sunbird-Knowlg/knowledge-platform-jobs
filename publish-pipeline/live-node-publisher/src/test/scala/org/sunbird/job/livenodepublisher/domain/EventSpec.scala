package org.sunbird.job.livenodepublisher.domain

import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.{FlatSpec, Matchers}
import org.scalatestplus.mockito.MockitoSugar
import org.sunbird.job.livenodepublisher.publish.domain.Event
import org.sunbird.job.livenodepublisher.task.LiveNodePublisherConfig
import org.sunbird.job.util.JSONUtil

class EventSpec extends FlatSpec with Matchers with MockitoSugar {

  val config: Config = ConfigFactory.load("test.conf").withFallback(ConfigFactory.systemEnvironment())
  val jobConfig: LiveNodePublisherConfig = new LiveNodePublisherConfig(config)

  "isValid" should "return true for a valid event" in {
    val sunbirdEvent = "{\"eid\":\"BE_JOB_REQUEST\",\"ets\":1619527882745,\"mid\":\"LP.1619527882745.32dc378a-430f-49f6-83b5-bd73b767ad36\",\"actor\":{\"id\":\"collection-publish\",\"type\":\"System\"},\"context\":{\"channel\":\"ORG_001\",\"pdata\":{\"id\":\"org.sunbird.platform\",\"ver\":\"1.0\"},\"env\":\"dev\"},\"object\":{\"id\":\"do_11361948306824396811\",\"ver\":\"1619153418829\"},\"edata\":{\"publish_type\":\"public\",\"metadata\":{\"identifier\":\"do_11361948306824396811\",\"mimeType\":\"application/vnd.ekstep.content-collection\",\"objectType\":\"Collection\",\"lastPublishedBy\":\"\",\"pkgVersion\":1},\"action\":\"republish\",\"iteration\":1}}"
    val event = new Event(JSONUtil.deserialize[java.util.Map[String, Any]](sunbirdEvent),0,1)

    assert(event.validEvent(jobConfig))
  }
}