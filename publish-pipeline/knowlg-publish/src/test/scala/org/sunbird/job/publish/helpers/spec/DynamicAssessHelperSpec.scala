package org.sunbird.job.publish.helpers.spec

import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.{FlatSpec, Matchers}
import org.scalatestplus.mockito.MockitoSugar
import org.sunbird.job.knowlg.publish.helpers.DynamicAssessHelper
import org.sunbird.job.knowlg.task.KnowlgPublishConfig
import org.sunbird.job.util.HttpUtil

class DynamicAssessHelperSpec extends FlatSpec with Matchers with MockitoSugar {

  val config: Config = ConfigFactory.load("test.conf").withFallback(ConfigFactory.systemEnvironment())
  val jobConfig: KnowlgPublishConfig = new KnowlgPublishConfig(config)
  val helper = new DynamicAssessHelper(jobConfig, mock[HttpUtil])

  "generatedMetadataNodeId" should "target the node itself for a never-published Draft" in {
    helper.generatedMetadataNodeId("do_1", "do_1", editCopyExists = false) shouldBe Some("do_1")
  }

  it should "target the edit copy when published content has one" in {
    helper.generatedMetadataNodeId("do_1.img", "do_1", editCopyExists = true) shouldBe Some("do_1.img")
  }

  it should "skip the write when published content has no edit copy, instead of failing on a missing node or touching live" in {
    helper.generatedMetadataNodeId("do_1.img", "do_1", editCopyExists = false) shouldBe None
  }

  "generatedContentMetadata" should "carry the question count the editor reads to know questions exist" in {
    helper.generatedContentMetadata(5)("totalQuestions") shouldBe Int.box(5)
  }

  it should "stamp lastUpdatedOn in the platform's timestamp format so a same-count regenerate is still detectable" in {
    val stamp = helper.generatedContentMetadata(5, new java.util.Date(0L))("lastUpdatedOn").toString
    stamp should fullyMatch regex """\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}[+-]\d{4}"""
  }
}
