package org.sunbird.job.publish.helpers

import java.io.{File, InputStream}
import scala.io.Source
import scala.util.{Success, Failure, Try}
import org.slf4j.LoggerFactory
import org.sunbird.job.util.ScalaJsonUtil

/** Field definition for enriched metadata.
  * @param name Field name
  * @param fieldType Data type (string, array, object, etc)
  * @param required Whether field is mandatory
  * @param description Human-readable field description
  */
case class FieldDefinition(
    name: String,
    fieldType: String,
    required: Boolean,
    description: String
)

/** Object type field configuration.
  * @param objectType Object type name (Content, Collection, Question, QuestionSet)
  * @param requiredFields Mandatory fields for this type
  * @param optionalFields Optional fields for this type
  * @param excludedFields Fields to exclude from enriched events (PII, URLs, etc)
  */
case class ObjectTypeFields(
    objectType: String,
    requiredFields: Seq[FieldDefinition],
    optionalFields: Seq[FieldDefinition],
    excludedFields: Seq[String]
) {
  /** All fields (required + optional). */
  def allFields: Seq[FieldDefinition] = requiredFields ++ optionalFields

  /** Get field names, optionally including optional fields. */
  def getFieldNames(includeOptional: Boolean = true): Seq[String] = {
    if (includeOptional) allFields.map(_.name)
    else requiredFields.map(_.name)
  }
}

/** Loads and manages field configurations for enriched metadata.
  * Reads enriched-metadata-fields.json to determine which fields to extract and emit for each object type.
  * @param configPath Path to field configuration JSON (classpath or filesystem)
  */
class FieldConfiguration(configPath: String = "enriched-metadata-fields.json") {
  private val logger = LoggerFactory.getLogger(classOf[FieldConfiguration])

  private val config: Map[String, ObjectTypeFields] = loadConfiguration(configPath)

  logger.info(s"FieldConfiguration loaded from $configPath. Configured object types: ${config.keys.mkString(", ")}")

  private def loadConfiguration(path: String): Map[String, ObjectTypeFields] = {
    Try {
      val jsonString =
        if (path.startsWith("classpath:")) {
          // Explicit classpath prefix
          loadFromClasspath(path.replace("classpath:", ""))
        } else {
          // Try classpath first (works in JAR), then fallback to filesystem (works in IDE)
          try {
            loadFromClasspath(path)
          } catch {
            case _: Exception => Source.fromFile(new File(path)).mkString
          }
        }

      val config = ScalaJsonUtil.deserialize[Map[String, Any]](jsonString)
      val objectTypes = config.getOrElse("object_types", Map.empty).asInstanceOf[Map[String, Any]]

      objectTypes.map { case (typeName, typeConfig) =>
        typeName -> parseObjectType(typeName, typeConfig.asInstanceOf[Map[String, Any]])
      }
    } match {
      case Success(cfg) =>
        logger.info(s"Successfully loaded field configuration for ${cfg.size} object types")
        cfg
      case Failure(e) =>
        logger.error(s"Failed to load field configuration from $path", e)
        throw new RuntimeException(s"Cannot load field configuration: ${e.getMessage}", e)
    }
  }

  private def loadFromClasspath(resourcePath: String): String = {
    val inputStream: InputStream = getClass.getClassLoader.getResourceAsStream(resourcePath)
    if (inputStream == null) {
      throw new RuntimeException(s"Resource not found on classpath: $resourcePath")
    }
    Source.fromInputStream(inputStream).mkString
  }

  private def parseObjectType(typeName: String, config: Map[String, Any]): ObjectTypeFields = {
    val fieldsObj = config.getOrElse("fields", Map.empty).asInstanceOf[Map[String, Any]]
    val excludedFields = config.getOrElse("excluded_fields", Seq.empty).asInstanceOf[Seq[String]]

    val allFields = fieldsObj.flatMap { case (categoryName, categoryFields) =>
      val categoryObj = categoryFields.asInstanceOf[Map[String, Any]]
      categoryObj.map { case (fieldName, fieldDef) =>
        val fieldObj = fieldDef.asInstanceOf[Map[String, Any]]
        FieldDefinition(
          name = fieldName,
          fieldType = fieldObj.getOrElse("type", "string").asInstanceOf[String],
          required = fieldObj.getOrElse("required", false).asInstanceOf[Boolean],
          description = fieldObj.getOrElse("description", "").asInstanceOf[String]
        )
      }
    }

    val requiredFields = allFields.filter(_.required).toSeq
    val optionalFields = allFields.filter(!_.required).toSeq

    ObjectTypeFields(
      objectType = typeName,
      requiredFields = requiredFields,
      optionalFields = optionalFields,
      excludedFields = excludedFields
    )
  }

  /** Get field definitions for object type.
    * @param objectType Object type to query
    * @param includeOptional Include optional fields (default true)
    * @return Sequence of field definitions
    */
  def getFieldsFor(objectType: String, includeOptional: Boolean = true): Seq[FieldDefinition] = {
    config.get(objectType) match {
      case Some(fields) =>
        if (includeOptional) fields.allFields
        else fields.requiredFields
      case None =>
        logger.warn(s"No field configuration found for object type: $objectType")
        Seq.empty
    }
  }

  /** Get required fields for object type. */
  def getRequiredFieldsFor(objectType: String): Seq[FieldDefinition] = {
    config.get(objectType) match {
      case Some(fields) => fields.requiredFields
      case None =>
        logger.warn(s"No field configuration found for object type: $objectType")
        Seq.empty
    }
  }

  /** Get field names for object type. */
  def getFieldNamesFor(objectType: String, includeOptional: Boolean = true): Seq[String] = {
    getFieldsFor(objectType, includeOptional).map(_.name)
  }

  /** Get excluded fields for object type (fields not to emit). */
  def getExcludedFieldsFor(objectType: String): Seq[String] = {
    config.get(objectType) match {
      case Some(fields) => fields.excludedFields
      case None =>
        logger.warn(s"No field configuration found for object type: $objectType")
        Seq.empty
    }
  }

  /** Check if field is excluded. */
  def isFieldExcluded(objectType: String, fieldName: String): Boolean = {
    getExcludedFieldsFor(objectType).contains(fieldName)
  }

  /** Check if field is required. */
  def isFieldRequired(objectType: String, fieldName: String): Boolean = {
    getRequiredFieldsFor(objectType).exists(_.name == fieldName)
  }

  /** Get data type of a field. */
  def getFieldType(objectType: String, fieldName: String): Option[String] = {
    getFieldsFor(objectType).find(_.name == fieldName).map(_.fieldType)
  }

  def validateObjectFields(objectType: String, fields: Map[String, Any]): Try[Unit] = {
    Try {
      val requiredFields = getRequiredFieldsFor(objectType)
      val excludedFields = getExcludedFieldsFor(objectType)

      requiredFields.foreach { reqField =>
        if (!fields.contains(reqField.name)) {
          throw new IllegalArgumentException(s"Required field missing for $objectType: ${reqField.name}")
        }
      }

      fields.keys.foreach { fieldName =>
        if (excludedFields.contains(fieldName)) {
          throw new IllegalArgumentException(s"Excluded field present for $objectType: $fieldName")
        }
      }

      logger.debug(s"Field validation passed for $objectType")
    }
  }

  def getObjectTypeConfig(objectType: String): Option[ObjectTypeFields] = {
    config.get(objectType)
  }

  def getAllConfiguredObjectTypes: Seq[String] = {
    config.keys.toSeq.sorted
  }
}
