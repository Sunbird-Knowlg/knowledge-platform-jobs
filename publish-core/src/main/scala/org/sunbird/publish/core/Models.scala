package org.sunbird.publish.core

case class ObjectData(metadata: Map[String, AnyRef], extData: Option[Map[String, AnyRef]] = None, hierarchy: Option[Map[String, AnyRef]] = None)
