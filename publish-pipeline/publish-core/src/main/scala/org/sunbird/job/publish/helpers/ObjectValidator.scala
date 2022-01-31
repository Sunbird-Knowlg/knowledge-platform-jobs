package org.sunbird.job.publish.helpers

import org.slf4j.LoggerFactory
import org.sunbird.job.publish.config.PublishConfig
import org.sunbird.job.publish.core.ObjectData

import scala.collection.mutable.ListBuffer

trait ObjectValidator {

  private[this] val logger = LoggerFactory.getLogger(classOf[ObjectValidator])

  def validate(obj: ObjectData, identifier: String, customFn: (ObjectData, String) => List[String]): List[String] = {
    validate(obj, identifier) ++ customFn(obj, identifier)
  }

  def validate(obj: ObjectData, identifier: String, config: PublishConfig, customFn: (ObjectData, String, PublishConfig) => List[String]): List[String] = {
    validate(obj, identifier) ++ customFn(obj, identifier, config)
  }

  def validate(obj: ObjectData, identifier: String): List[String] = {
    logger.info("Validating object with id: " + obj.identifier)
    val messages = ListBuffer[String]()
    if (obj.metadata.isEmpty) messages += s"""There is no metadata available for : $identifier"""
    if (obj.metadata.get("mimeType").isEmpty) messages += s"""There is no mimeType defined for : $identifier"""
    if (obj.metadata.get("primaryCategory").isEmpty) messages += s"""There is no primaryCategory defined for : $identifier"""
    if (obj.metadata.get("name").isEmpty) messages += s"""There is no name defined for : $identifier"""
    if (obj.metadata.get("code").isEmpty) messages += s"""There is no code defined for : $identifier"""
    messages.toList
  }
}
