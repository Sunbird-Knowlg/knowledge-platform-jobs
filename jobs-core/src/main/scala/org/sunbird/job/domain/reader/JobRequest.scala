package org.sunbird.job.domain.reader

import org.sunbird.job.util.JSONUtil

abstract class JobRequest(val map: java.util.Map[String, Any]) {

  def getMap(): java.util.Map[String, Any] = map

  def getJson(): String = JSONUtil.serialize(getMap())

  def mid(): String = read[String](keyPath = EventsPath.MID_PATH).orNull

  def kafkaKey(): String = mid()

  def read[T](keyPath: String): Option[T] = try {
    val parentMap = lastParentMap(map, keyPath)
    Option(parentMap.readChild.orNull.asInstanceOf[T])
  } catch {
    case ex: Exception =>
      None
  }

  def readOrDefault[T](keyPath: String, defaultValue: T): T = {
    read(keyPath).getOrElse(defaultValue)
  }

  @throws[JobRequestReaderException]
  def mustReadValue[T](keyPath: String): T = {
    read(keyPath).getOrElse({
      val mid = read("mid")
      throw new JobRequestReaderException(s"keyPath is not available in the $mid ")
    })
  }

  override def toString: String = "JobRequest {map=" + map + "}"

  private def lastParentMap(map: java.util.Map[String, Any], keyPath: String): ParentType = {
    try {
      var parent = map
      val keys = keyPath.split("\\.")
      val lastIndex = keys.length - 1
      if (keys.length > 1) {
        var i = 0
        while ( {
          i < lastIndex && parent != null
        }) {
          var result: java.util.Map[String, Any] = null
          if (parent.isInstanceOf[java.util.Map[_, _]]) result = new ParentMap(parent, keys(i)).readChild.orNull
          parent = result
          i += 1
        }
      }
      val lastKeyInPath = keys(lastIndex)
      if (parent.isInstanceOf[java.util.Map[_, _]]) new ParentMap(parent, lastKeyInPath)
      else null
    } catch {
      case ex: Exception =>
        null
    }
  }
}

class JobRequestReaderException(val message: String) extends Exception(message) {}