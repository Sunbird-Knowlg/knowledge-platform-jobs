package org.sunbird.job.publish.helpers.spec

import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.{FlatSpec, Matchers}
import org.sunbird.job.cache.{DataCache, RedisConnect}
import org.sunbird.job.knowlg.task.KnowlgPublishConfig
import org.sunbird.job.util.CassandraUtil

import scala.collection.JavaConverters._

/**
 * Exercises only the Redis write path of CollectionPublisher.storeRelationshipData.
 * No Cassandra, no JanusGraph, no cloud storage - just a local Redis.
 */
class CollectionPublisherRedisSpec extends FlatSpec with Matchers {

  // Force redis.enabled=true for this spec regardless of what content-publish.conf/test.conf currently say.
  val config: Config = ConfigFactory.parseString("redis.enabled = true")
    .withFallback(ConfigFactory.load("test.conf").withFallback(ConfigFactory.systemEnvironment()))
  val jobConfig = new KnowlgPublishConfig(config)
  implicit val cassandraUtil: CassandraUtil = null // never touched on the redis-enabled branch

  val rootId = "do_redis_test_root"
  val unitId = "do_redis_test_unit1"
  val leaf1Id = "do_redis_test_leaf1"
  val leaf2Id = "do_redis_test_leaf2"

  "storeRelationshipData" should "write leaf/optional/ancestor node ids to Redis when redis is enabled" in {
    val redisConnect = new RedisConnect(jobConfig)
    val dataCache = new DataCache(jobConfig, redisConnect, jobConfig.hierarchyRelationsDbId, List())
    dataCache.init()
    val jedis = redisConnect.getConnection(jobConfig.hierarchyRelationsDbId)
    try {
      val publisher = new TestCollectionPublisher()
      publisher.testStoreRelationshipData(rootId, "leafnodes", Map(unitId -> List(leaf1Id, leaf2Id)), dataCache)(cassandraUtil, jobConfig)
      publisher.testStoreRelationshipData(rootId, "optionalnodes", Map(unitId -> List(leaf2Id)), dataCache)(cassandraUtil, jobConfig)
      publisher.testStoreRelationshipData(rootId, "ancestors", Map(leaf1Id -> List(unitId, rootId)), dataCache)(cassandraUtil, jobConfig)

      jedis.smembers(s"$rootId:$unitId:leafnodes").asScala should contain theSameElementsAs List(leaf1Id, leaf2Id)
      jedis.smembers(s"$rootId:$unitId:optionalnodes").asScala should contain theSameElementsAs List(leaf2Id)
      jedis.smembers(s"$rootId:$leaf1Id:ancestors").asScala should contain theSameElementsAs List(unitId, rootId)
    } finally {
      jedis.del(s"$rootId:$unitId:leafnodes", s"$rootId:$unitId:optionalnodes", s"$rootId:$leaf1Id:ancestors")
      jedis.close()
      dataCache.close()
    }
  }

}
