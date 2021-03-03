package org.sunbird.job.models

import java.util
import scala.collection.JavaConverters._

import org.sunbird.job.task.CompositeSearchIndexerConfig

//case class SearchIndexer(userId: String, batchId: String, courseId: String, progress: Int, completedOn: Date, contentStatus: Map[String, Int], inputContents: List[String], completed: Boolean = false)

case class CompositeIndexer(graphId: String, objectType: String, uniqueId: String, messageId: String, version: Double, message: util.Map[String, Any], config: CompositeSearchIndexerConfig) {
  def getNestedFields(): List[String] = config.nestedFields.asScala.toList
  def getDefinitionBasePath(): String = config.definitionBasePath
  def getVersionAsString(): String = version.toString
  def getRestrictMetadataObjectTypes(): List[String] = config.restrictMetadataObjectTypes.asScala.toList
}
