package org.sunbird.publish.helpers

import org.sunbird.publish.core.ObjectData

trait ObjectValidator {

  def validateObject(obj: ObjectData, identifier: String, customFn: (ObjectData, String) => Unit): Unit = {
    validateObject(obj, identifier)
    customFn(obj, identifier)
  }

  def validateObject(obj: ObjectData, identifier: String): Unit = {
    if (obj.metadata.isEmpty) throw new Exception("There is no metadata available for : " + identifier)
    if (obj.metadata.get("mimeType").isEmpty) throw new Exception("There is no mimeType defined for : " + identifier)
    
  }
}
