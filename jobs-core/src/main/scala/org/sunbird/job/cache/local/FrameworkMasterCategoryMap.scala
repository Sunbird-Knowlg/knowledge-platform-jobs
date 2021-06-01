package org.sunbird.job.cache.local

import com.twitter.storehaus.cache.Cache
import com.twitter.util.Duration

object FrameworkMasterCategoryMap {

  val ttlMS = 100000l//Platform.getLong("master.category.cache.ttl", 10000l)
  var cache =  Cache.ttl[String, Map[String, AnyRef]](Duration.fromMilliseconds(ttlMS))

  def get(id: String):Map[String, AnyRef] = {
    cache.getNonExpired(id).getOrElse(null)
  }

  def put(id: String, data: Map[String, AnyRef]): Unit = {
    val updated = cache.putClocked(id, data)._2
    cache = updated
  }

  def containsKey(id: String): Boolean = {
    cache.contains(id)
  }
}
