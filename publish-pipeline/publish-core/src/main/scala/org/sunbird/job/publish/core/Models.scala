package org.sunbird.job.publish.core

class ObjectData(val identifier: String, val metadata: Map[String, AnyRef], val extData: Option[Map[String, AnyRef]] = None, val hierarchy: Option[Map[String, AnyRef]] = None) {

  val dbId = metadata.getOrElse("identifier", identifier).asInstanceOf[String]

  val dbObjType = metadata.getOrElse("objectType", "").asInstanceOf[String]

  val pkgVersion = metadata.getOrElse("pkgVersion", 0.0.asInstanceOf[Number]).asInstanceOf[Number].intValue()

}

case class ExtDataConfig(keyspace: String, table: String, primaryKey:List[String] = List(), propsMapping: Map[String, AnyRef] = Map())

case class DefinitionConfig(supportedVersion: Map[String, AnyRef], basePath: String)

case class ObjectExtData(data:Option[Map[String, AnyRef]] = None, hierarchy: Option[Map[String, AnyRef]] = None)
