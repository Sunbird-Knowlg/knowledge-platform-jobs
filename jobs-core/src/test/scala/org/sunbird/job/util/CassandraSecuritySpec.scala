package org.sunbird.job.util

import com.datastax.driver.core.{AuthProvider, Cluster, PlainTextAuthProvider}
import org.scalatest.{FlatSpec, Matchers}

class CassandraSecuritySpec extends FlatSpec with Matchers {

  private def resource(name: String): String =
    new java.io.File(getClass.getClassLoader.getResource(name).toURI).getAbsolutePath

  private def builder: Cluster.Builder = Cluster.builder().addContactPoint("127.0.0.1")

  private def protocol(b: Cluster.Builder) = b.getConfiguration.getProtocolOptions

  "configure with defaults" should "leave the builder untouched" in {
    val b = CassandraSecurity.configure(builder, "", "", false, "")
    protocol(b).getSSLOptions shouldBe null
    protocol(b).getAuthProvider should be theSameInstanceAs AuthProvider.NONE
  }

  "configure with a username but no password" should "skip credentials" in {
    val b = CassandraSecurity.configure(builder, "admin", "", false, "")
    protocol(b).getAuthProvider should be theSameInstanceAs AuthProvider.NONE
  }

  "configure with username and password" should "set credentials" in {
    val b = CassandraSecurity.configure(builder, "admin", "secret", false, "")
    protocol(b).getAuthProvider shouldBe a[PlainTextAuthProvider]
  }

  "configure with ssl enabled" should "set ssl options" in {
    val b = CassandraSecurity.configure(builder, "", "", true, resource("test-root.crt"))
    protocol(b).getSSLOptions should not be null
  }

  "configure with ssl enabled and no cert" should "fail" in {
    an[IllegalStateException] should be thrownBy CassandraSecurity.configure(builder, "", "", true, "")
  }

  "buildSslContext" should "load a single certificate" in {
    CassandraSecurity.buildSslContext(resource("test-root.crt")) should not be null
  }

  it should "load a certificate bundle" in {
    CassandraSecurity.buildSslContext(resource("test-bundle.crt")) should not be null
  }

  it should "fail for a missing file" in {
    an[IllegalStateException] should be thrownBy CassandraSecurity.buildSslContext("/does/not/exist.crt")
  }

  it should "fail for a blank path" in {
    an[IllegalStateException] should be thrownBy CassandraSecurity.buildSslContext(null)
  }
}
