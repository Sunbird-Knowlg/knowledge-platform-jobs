package org.sunbird.job.publish.helpers

import com.typesafe.config.Config
import scala.collection.JavaConverters._
import scala.util.{Success, Failure, Try}
import org.slf4j.LoggerFactory

/** Loads and manages field configurations for enriched metadata.
  * Reads from HOCON config with layered structure: global fields + type-specific fields.
  * @param typesafeConfig HOCON configuration object
  */
class FieldConfiguration(typesafeConfig: Config) {
  private val logger = LoggerFactory.getLogger(classOf[FieldConfiguration])

  // Load global fields (apply to all types)
  private val globalFields: Seq[String] = Try {
    if (typesafeConfig.hasPath("enriched.metadata.global.includeFields")) {
      typesafeConfig.getStringList("enriched.metadata.global.includeFields").asScala.toSeq
    } else {
      Seq.empty
    }
  }.getOrElse(Seq.empty)

  // Load type-specific fields
  private val typeFields: Map[String, Seq[String]] = loadTypeFields()

  logger.info(s"FieldConfiguration loaded. Global fields: ${globalFields.size}. Configured types: ${typeFields.keys.mkString(", ")}")

  private def loadTypeFields(): Map[String, Seq[String]] = {
    Try {
      val types = Seq("content", "collection", "question", "questionset")
      types.flatMap { typeName =>
        val path = s"enriched.metadata.$typeName.includeFields"
        if (typesafeConfig.hasPath(path)) {
          Some(typeName -> typesafeConfig.getStringList(path).asScala.toSeq)
        } else {
          None
        }
      }.toMap
    } match {
      case Success(fields) => fields
      case Failure(e) =>
        logger.warn(s"Error loading type-specific fields from config", e)
        Map.empty
    }
  }

  /** Get all field names for object type (global + type-specific).
    * @param objectType Object type (Content, Collection, Question, QuestionSet)
    * @return Combined list of field names
    */
  def getFieldNamesFor(objectType: String): Seq[String] = {
    val typeSpecific = typeFields.get(objectType.toLowerCase).getOrElse(Seq.empty)
    (globalFields ++ typeSpecific).distinct
  }

  /** Check if hierarchy should be included for object type.
    * @param objectType Object type to query
    * @return True if includeHierarchy=true for this type
    */
  def shouldIncludeHierarchy(objectType: String): Boolean = {
    Try {
      typesafeConfig.getBoolean(s"enriched.metadata.${objectType.toLowerCase}.includeHierarchy")
    }.getOrElse(false)
  }

  /** Get all configured object types. */
  def getAllConfiguredObjectTypes: Seq[String] = {
    typeFields.keys.toSeq.sorted
  }
}
