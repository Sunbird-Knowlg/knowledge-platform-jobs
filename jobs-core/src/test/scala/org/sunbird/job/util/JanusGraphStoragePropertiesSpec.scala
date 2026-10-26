package org.sunbird.job.util

import com.typesafe.config.ConfigFactory
import org.scalatest.{FlatSpec, Matchers}
import org.sunbird.job.BaseJobConfig

class JanusGraphStoragePropertiesSpec extends FlatSpec with Matchers {

  private def props(overrides: String) = {
    val config = ConfigFactory.parseString(overrides).withFallback(ConfigFactory.load("base-test.conf"))
    JanusGraphUtil.storageProperties(new BaseJobConfig(config, "test"))
  }

  "storageProperties with defaults" should "add no credentials or ssl settings" in {
    val p = props("")
    p.get("storage.backend") shouldBe "cql"
    p.containsKey("storage.username") shouldBe false
    p.containsKey("storage.password") shouldBe false
    p.containsKey("storage.cql.ssl.enabled") shouldBe false
  }

  "storageProperties with credentials" should "pass username and password" in {
    val p = props("janusgraph.storage.username = admin\njanusgraph.storage.password = secret")
    p.get("storage.username") shouldBe "admin"
    p.get("storage.password") shouldBe "secret"
  }

  "storageProperties with a username but no password" should "skip credentials" in {
    props("janusgraph.storage.username = admin").containsKey("storage.username") shouldBe false
  }

  "storageProperties with ssl enabled" should "pass the truststore settings" in {
    val p = props(
      """janusgraph.storage.cql.ssl.enabled = true
        |janusgraph.storage.cql.ssl.truststore.location = /certs/truststore.jks
        |janusgraph.storage.cql.ssl.truststore.password = changeit""".stripMargin)
    p.get("storage.cql.ssl.enabled") shouldBe java.lang.Boolean.TRUE
    p.get("storage.cql.ssl.truststore.location") shouldBe "/certs/truststore.jks"
    p.get("storage.cql.ssl.truststore.password") shouldBe "changeit"
  }
}
