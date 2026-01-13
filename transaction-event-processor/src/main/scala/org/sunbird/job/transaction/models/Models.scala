package org.sunbird.job.transaction.models

import org.sunbird.job.transaction.task.TransactionEventProcessorConfig

import java.util
import scala.collection.JavaConverters._

case class CompositeIndexer(
    graphId: String,
    objectType: String,
    identifier: String,
    messageId: String,
    message: util.Map[String, Any],
    config: TransactionEventProcessorConfig
) {
  def getNestedFields(): List[String] = config.nestedFields.asScala.toList
  def getDefinitionBasePath(): String = config.definitionBasePath
  def getVersionAsString(): String = config.schemaSupportVersionMap
    .getOrElse(objectType.toLowerCase(), "1.0")
    .asInstanceOf[String]
  def getRestrictMetadataObjectTypes(): List[String] =
    config.restrictMetadataObjectTypes.asScala.toList
  def getIgnoredFields(): List[String] = config.ignoredFields
}
