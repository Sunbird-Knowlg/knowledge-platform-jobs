package org.sunbird.spec

// import com.opentable.db.postgres.embedded.EmbeddedPostgres
import org.scalatest.{BeforeAndAfterAll, FlatSpec}
import redis.embedded.RedisServer

class BaseSpec extends FlatSpec with BeforeAndAfterAll {
  var redisServer: RedisServer = _

  override def beforeAll() {
    super.beforeAll()
    redisServer = RedisServer.builder().port(6340).setting("maxheap 128M").build()
    try {
      redisServer.start()
    } catch {
      case e: Throwable => Console.err.println("Failed to start embedded redis server: " + e.getMessage)
    }
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    if (redisServer != null) {
      try {
        redisServer.stop()
      } catch {
        case e: Throwable => Console.err.println("Failed to stop embedded redis server: " + e.getMessage)
      }
    }
  }

}
