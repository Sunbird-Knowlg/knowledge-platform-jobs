package org.sunbird.publish.helpers

import org.sunbird.publish.core.ObjectData

import scala.collection.mutable.ListBuffer

trait ObjectValidator {

  def validate(obj: ObjectData, identifier: String, customFn: (ObjectData, String) => List[String]): Unit = {
    validate(obj, identifier) ++ customFn(obj, identifier)
  }

  def validate(obj: ObjectData, identifier: String): List[String] = {
    val messages = ListBuffer[String]()
    if (obj.metadata.isEmpty) messages += s"""There is no metadata available for : $identifier"""
    if (obj.metadata.get("mimeType").isEmpty) messages += s"""There is no mimeType defined for : $identifier"""
    messages.toList
  }
}
