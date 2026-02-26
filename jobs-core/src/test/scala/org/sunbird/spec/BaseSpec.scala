package org.sunbird.spec

// import com.opentable.db.postgres.embedded.EmbeddedPostgres
import org.scalatest.{BeforeAndAfterAll, FlatSpec}
import redis.embedded.RedisServer

class BaseSpec extends FlatSpec with BeforeAndAfterAll {
  var redisServer: RedisServer = _
  var redisAvailable: Boolean = false

  override def beforeAll() {
    super.beforeAll()
    try {
      redisServer = new RedisServer(6340)
      redisServer.start()
      redisAvailable = true
    } catch {
      case e: Exception =>
        println(s"WARNING: Could not start embedded Redis on port 6340: ${e.getMessage}. Redis-dependent tests will be cancelled.")
    }
    // EmbeddedPostgres.builder.setPort(5430).start() // Use the same port 5430 which is defined in the base-test.conf
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    if (redisAvailable && redisServer != null) redisServer.stop()
  }

}
