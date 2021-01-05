package org.sunbird.publish.core

class ObjectData(val identifier: String, val metadata: Map[String, AnyRef], val extData: Option[Map[String, AnyRef]] = None, val hierarchy: Option[Map[String, AnyRef]] = None) {

  val dbId = metadata.get("IL_UNIQUE_ID").get.asInstanceOf[String]

  val pkgVersion = metadata.getOrElse("pkgVersion", 0.0.asInstanceOf[Number]).asInstanceOf[Number].intValue()

}
