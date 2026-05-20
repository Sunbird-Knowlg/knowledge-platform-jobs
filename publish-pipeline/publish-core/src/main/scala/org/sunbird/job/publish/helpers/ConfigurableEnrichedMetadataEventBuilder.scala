package org.sunbird.job.publish.helpers

import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.JavaConverters._
import org.slf4j.LoggerFactory
import org.sunbird.job.publish.core.ObjectData
import org.sunbird.job.util.ScalaJsonUtil

/** Enriched metadata event for semantic search.
  * @param id Object identifier
  * @param contentType Object type (Content, Collection, Question, QuestionSet)
  * @param _schema_version Event schema version
  * @param data Extracted and filtered metadata fields
  */
case class EnrichedMetadataEvent(
    id: String,
    contentType: String,
    _schema_version: String,
    data: Map[String, Any]
)

/** Builds enriched metadata events for downstream semantic search embeddings.
  * Extracts configured fields from objects, optionally includes hierarchy, and emits to Kafka.
  * @param fieldConfig Field configuration defining which fields to extract per object type
  * @param enrichedMetadataTopic Kafka topic for emitting enriched metadata events
  * @param includeHierarchy Whether to include hierarchy for collections/questionsets (default false)
  */
class ConfigurableEnrichedMetadataEventBuilder(
    fieldConfig: FieldConfiguration,
    enrichedMetadataTopic: String,
    includeHierarchy: Boolean = false
) {
  private val logger = LoggerFactory.getLogger(classOf[ConfigurableEnrichedMetadataEventBuilder])

  private val SCHEMA_VERSION = "1.0"

  /** Build enriched metadata event from object.
    * @param obj Object to enrich
    * @return EnrichedMetadataEvent with extracted and filtered fields
    */
  def buildEnrichedMetadataEvent(obj: ObjectData): EnrichedMetadataEvent = {
    val objectType = getObjectType(obj)
    logger.info(s"Building enriched metadata event for $objectType: ${obj.identifier}")

    var enrichedData = extractConfiguredFields(obj, objectType)
    logger.debug(s"Extracted ${enrichedData.size} fields for $objectType: ${obj.identifier}")

    if (includeHierarchy && isHierarchyIncludeableType(objectType) && obj.hierarchy.isDefined) {
      val filteredHierarchy = filterHierarchyFields(obj.hierarchy.get)
      enrichedData = enrichedData ++ Map("hierarchy" -> filteredHierarchy)
      logger.debug(s"Included hierarchy data for $objectType: ${obj.identifier}")
    }

    validateEnrichedData(objectType, enrichedData)
    logger.info(s"Enriched metadata validation passed for $objectType: ${obj.identifier}")

    EnrichedMetadataEvent(
      id = obj.identifier,
      contentType = objectType,
      _schema_version = SCHEMA_VERSION,
      data = enrichedData
    )
  }

  /** Build enriched metadata event ready for Kafka emission.
    * @param obj Object to enrich
    * @return Map with id, contentType, _schema_version, timestamp, and data
    */
  def buildEnrichedKafkaEvent(obj: ObjectData): Map[String, Any] = {
    val event = buildEnrichedMetadataEvent(obj)

    Map(
      "id" -> event.id,
      "contentType" -> event.contentType,
      "_schema_version" -> event._schema_version,
      "timestamp" -> System.currentTimeMillis(),
      "data" -> event.data
    )
  }

  private def getObjectType(obj: ObjectData): String = {
    val mimeType = obj.getString("mimeType", "")

    mimeType match {
      case "application/vnd.ekstep.content-collection" => "Collection"
      case "application/vnd.sunbird.question" => "Question"
      case "application/vnd.sunbird.questionset" => "QuestionSet"
      case _ =>
        "Content"
    }
  }

  private def isHierarchyIncludeableType(objectType: String): Boolean = {
    objectType match {
      case "Collection" => true
      case "QuestionSet" => true
      case _ => false
    }
  }

  private def filterHierarchyFields(hierarchy: Map[String, Any]): Map[String, Any] = {
    val objectType = hierarchy.get("objectType") match {
      case Some(ot: String) => ot
      case _ => getObjectTypeFromMimeType(hierarchy.get("mimeType").asInstanceOf[Option[String]])
    }

    val configuredFieldNames = fieldConfig.getFieldNamesFor(objectType, includeOptional = true)
    val excludedFields = fieldConfig.getExcludedFieldsFor(objectType)
    logger.debug(s"Filtering hierarchy node of type $objectType with ${configuredFieldNames.length} configured fields")

    // Filter this node's fields
    val filteredNode = hierarchy
      .filter { case (fieldName, _) =>
        configuredFieldNames.contains(fieldName) && !excludedFields.contains(fieldName)
      }
      .map { case (fieldName, value) =>
        fieldName -> sanitizeFieldValue(fieldName, value)
      }
      .filter { case (_, value) => value != null }

    logger.debug(s"Filtered hierarchy node: ${filteredNode.size} fields retained for type $objectType")

    // Recursively filter children if present
    val result = if (hierarchy.contains("children")) {
      val children = hierarchy("children")
      val filteredChildren = children match {
        case list: java.util.List[Any] @unchecked =>
          try {
            list.asScala.map { child =>
              child match {
                case m: java.util.Map[String, Any] @unchecked =>
                  filterHierarchyFields(m.asScala.toMap)
                case m: Map[String, Any] @unchecked =>
                  filterHierarchyFields(m)
                case _ => child
              }
            }.toList
          } catch {
            case _: Exception => children
          }
        case seq: Seq[Any] =>
          seq.map { child =>
            child match {
              case m: Map[String, Any] @unchecked =>
                filterHierarchyFields(m)
              case _ => child
            }
          }
        case _ => children
      }
      filteredNode ++ Map("children" -> filteredChildren)
    } else {
      filteredNode
    }

    result
  }

  private def getObjectTypeFromMimeType(mimeTypeOpt: Option[String]): String = {
    mimeTypeOpt match {
      case Some(mimeType) =>
        mimeType match {
          case "application/vnd.ekstep.content-collection" => "Collection"
          case "application/vnd.sunbird.question" => "Question"
          case "application/vnd.sunbird.questionset" => "QuestionSet"
          case _ => "Content"
        }
      case None => "Content"
    }
  }

  private def extractConfiguredFields(obj: ObjectData, objectType: String): Map[String, Any] = {
    val fieldsToExtract = fieldConfig.getFieldNamesFor(objectType, includeOptional = true)
    val excludedFields = fieldConfig.getExcludedFieldsFor(objectType)

    logger.debug(s"Extracting ${fieldsToExtract.length} fields for $objectType: ${fieldsToExtract.mkString(", ")}")
    logger.debug(s"Excluded fields for $objectType: ${excludedFields.mkString(", ")}")

    val extracted = obj.metadata
      .filter { case (fieldName, _) =>
        fieldsToExtract.contains(fieldName) && !excludedFields.contains(fieldName)
      }
      .map { case (fieldName, value) =>
        fieldName -> sanitizeFieldValue(fieldName, value)
      }
      .filter { case (_, value) => value != null }

    logger.debug(s"Extracted ${extracted.size} metadata fields from object metadata for $objectType: ${obj.identifier}")

    // Add identifier (top-level property, not in metadata)
    val withIdentifier = if (fieldsToExtract.contains("identifier")) {
      val result = extracted + ("identifier" -> obj.identifier)
      logger.debug(s"Added identifier field for $objectType: ${obj.identifier}")
      result
    } else extracted

    logger.debug(s"Final extracted fields for $objectType: ${withIdentifier.keySet.mkString(", ")}")
    withIdentifier
  }

  private def sanitizeFieldValue(fieldName: String, value: Any): AnyRef = {
    value match {
      case null => null
      case m: java.util.Map[String, Any] @unchecked => m.asScala.toMap.asInstanceOf[AnyRef]
      case l: java.util.List[Any] @unchecked => l.asScala.toList.asInstanceOf[AnyRef]
      case arr: Array[_] => arr.toList.asInstanceOf[AnyRef]
      case str: String => if (isEmptyDescription(str)) null else str.asInstanceOf[AnyRef]
      case _ => value.asInstanceOf[AnyRef]
    }
  }

  private def isEmptyDescription(str: String): Boolean = {
    str.trim.toLowerCase().startsWith("enter description")
  }

  private def validateEnrichedData(objectType: String, data: Map[String, Any]): Unit = {
    val requiredFields = fieldConfig.getRequiredFieldsFor(objectType).map(_.name)
    logger.debug(s"Required fields for $objectType: ${requiredFields.mkString(", ")}")

    val missingRequired = requiredFields.filter(!data.contains(_))
    if (missingRequired.nonEmpty) {
      logger.warn(s"Missing required fields for $objectType: ${missingRequired.mkString(", ")}")
    }

    val excludedFields = fieldConfig.getExcludedFieldsFor(objectType)
    logger.debug(s"Total excluded fields configured for $objectType: ${excludedFields.length}")

    val unwantedFields = data.keys.filter(excludedFields.contains)
    if (unwantedFields.nonEmpty) {
      logger.error(s"ERROR: Excluded fields present in enriched data for $objectType: ${unwantedFields.mkString(", ")}")
      logger.error(s"Data keys present: ${data.keys.mkString(", ")}")
      throw new IllegalStateException(s"Excluded fields leaked into enriched event: ${unwantedFields.mkString(", ")}")
    }

    logger.info(s"Enriched data validation passed for $objectType. Total fields: ${data.size}, Present fields: ${data.keys.mkString(", ")}")
  }

  /** Serialize enriched metadata event to JSON string.
    * @param event Event to serialize
    * @return JSON string representation
    */
  def serializeToJson(event: EnrichedMetadataEvent): String = {
    val eventMap = Map(
      "id" -> event.id,
      "contentType" -> event.contentType,
      "_schema_version" -> event._schema_version,
      "timestamp" -> System.currentTimeMillis(),
      "data" -> event.data
    )
    ScalaJsonUtil.serialize(eventMap)
  }

  def getSchemaVersion: String = SCHEMA_VERSION

  def getEnrichedMetadataTopic: String = enrichedMetadataTopic
}
