package org.sunbird.job.postpublish.config

import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.scalatestplus.mockito.MockitoSugar

class PostPublishConfigTestSpec extends FlatSpec with BeforeAndAfterAll with Matchers with MockitoSugar{
  val config: Config = ConfigFactory.load("test.conf")
  override protected def beforeAll(): Unit = {
    super.beforeAll()
  }
  override protected def afterAll(): Unit = {
    super.afterAll()
  }
  "getConfig" should "return valid config object" in {
    new PostPublishConfig(config, "job").getConfig().isInstanceOf[Config] should be (true)
  }
  "getString" should "return default value" in {
    new PostPublishConfig(config, "job").getString("abc", "abc") should be ("abc")
  }
  "getString" should "return valid value of key" in {
    new PostPublishConfig(config, "job").getString("dialcode-cassandra.keyspace", "abc") should be ("dialcodes")
  }
}