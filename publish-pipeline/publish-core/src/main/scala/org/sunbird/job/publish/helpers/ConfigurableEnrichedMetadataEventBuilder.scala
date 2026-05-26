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
  * @param globalIncludeHierarchy Global flag to enable hierarchy inclusion (can be overridden per-type)
  */
class ConfigurableEnrichedMetadataEventBuilder(
    fieldConfig: FieldConfiguration,
    enrichedMetadataTopic: String,
    globalIncludeHierarchy: Boolean = false
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

    if (shouldIncludeHierarchy(objectType) && obj.hierarchy.isDefined) {
      val filteredHierarchy = filterHierarchyFields(obj.hierarchy.get)
      enrichedData = enrichedData ++ Map("hierarchy" -> filteredHierarchy)
      logger.debug(s"Included hierarchy data for $objectType: ${obj.identifier}")
    }

    logger.info(s"Enriched metadata built for $objectType: ${obj.identifier} with ${enrichedData.size} fields")

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

  private def shouldIncludeHierarchy(objectType: String): Boolean = {
    globalIncludeHierarchy && fieldConfig.shouldIncludeHierarchy(objectType)
  }

  private def filterHierarchyFields(hierarchy: Map[String, Any]): Map[String, Any] = {
    val objectType = hierarchy.get("objectType") match {
      case Some(ot: String) => ot
      case _ => getObjectTypeFromMimeType(hierarchy.get("mimeType").asInstanceOf[Option[String]])
    }

    val configuredFieldNames = fieldConfig.getFieldNamesFor(objectType)
    logger.debug(s"Filtering hierarchy node of type $objectType with ${configuredFieldNames.length} configured fields")

    val filteredNode = hierarchy
      .filter { case (fieldName, _) =>
        configuredFieldNames.contains(fieldName) ||
          (fieldConfig.shouldAutoIncludeSearchEnrichmentFields && fieldConfig.isSearchEnrichmentField(fieldName))
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
    val fieldsToExtract = fieldConfig.getFieldNamesFor(objectType)

    logger.debug(s"Extracting ${fieldsToExtract.length} fields for $objectType: ${fieldsToExtract.mkString(", ")}")

    val extracted = obj.metadata
      .filter { case (fieldName, _) =>
        fieldsToExtract.contains(fieldName)
      }
      .map { case (fieldName, value) =>
        fieldName -> sanitizeFieldValue(fieldName, value)
      }
      .filter { case (_, value) => value != null }

    logger.debug(s"Extracted ${extracted.size} metadata fields from object metadata for $objectType: ${obj.identifier}")

    // Auto-include search enrichment fields (se_*) if enabled
    val withSearchEnrichment = if (fieldConfig.shouldAutoIncludeSearchEnrichmentFields) {
      val seFields = obj.metadata
        .filter { case (fieldName, _) =>
          fieldConfig.isSearchEnrichmentField(fieldName)
        }
        .map { case (fieldName, value) =>
          fieldName -> sanitizeFieldValue(fieldName, value)
        }
        .filter { case (_, value) => value != null }

      if (seFields.nonEmpty) {
        logger.debug(s"Auto-included ${seFields.size} search enrichment fields for $objectType: ${seFields.keys.mkString(", ")}")
      }
      extracted ++ seFields
    } else {
      extracted
    }

    val withIdentifier = if (fieldsToExtract.contains("identifier")) {
      val result = withSearchEnrichment + ("identifier" -> obj.identifier)
      logger.debug(s"Added identifier field for $objectType: ${obj.identifier}")
      result
    } else withSearchEnrichment

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
    logger.info(s"Enriched data validation passed for $objectType. Total fields: ${data.size}, Fields: ${data.keys.mkString(", ")}")
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
