package org.sunbird.job.contentembedding.strategy

import org.slf4j.LoggerFactory
import org.sunbird.job.contentembedding.domain.{ChunkingConfig, TextChunk}
import org.sunbird.job.contentembedding.service.ChunkingStrategy

/**
 * Field-based chunking strategy — one semantically cohesive chunk per content section.
 *
 * Unlike sliding-window, this strategy does NOT split on token count. Instead it maps
 * Sunbird content types to a fixed set of meaningful fields and produces one chunk
 * per logical section, truncating at `config.maxChunkSize` characters.
 *
 * Chunking rules by content type:
 *  - '''Content'''    → 1 chunk: name + description + keywords + subject
 *  - '''Question'''   → 1 chunk: name + description + body + subject
 *  - '''Collection''' → 1 metadata chunk + 1 chunk per hierarchy child (recursive)
 *  - '''QuestionSet'''→ 1 metadata chunk + 1 chunk per hierarchy child (recursive)
 *
 * Best suited for short-to-medium metadata. Use `SlidingWindowChunkingStrategy` for
 * long documents (e.g. full article body > 512 tokens).
 *
 * @param config Chunking config; only `maxChunkSize` (character limit) is used.
 */
class SemanticChunkingStrategy(config: ChunkingConfig = ChunkingConfig("semantic")) extends ChunkingStrategy {

  private[this] val logger = LoggerFactory.getLogger(classOf[SemanticChunkingStrategy])

  override def getName: String = "semantic"

  override def getVersion: String = "1.0"

  private def truncate(text: String): String =
    if (text.length > config.maxChunkSize) text.take(config.maxChunkSize) else text

  private def getSafeString(data: Map[String, Any], key: String): String = {
    data.get(key) match {
      case Some(value) if value != null => value.toString
      case _ => ""
    }
  }

  private def extractListValues(data: Map[String, Any], key: String): String = {
    data.get(key) match {
      case Some(seq: Seq[_]) => seq.map(v => if (v != null) v.toString else "").filter(_.nonEmpty).mkString(", ")
      case Some(value) if value != null => value.toString
      case _ => ""
    }
  }

  override def chunk(
      objectId: String,
      contentType: String,
      data: Map[String, Any]
  ): List[TextChunk] = {
    logger.debug(s"Chunking $contentType:$objectId using semantic strategy (maxChunkSize=${config.maxChunkSize})")

    contentType match {
      case "Content"     => chunkContent(objectId, data)
      case "Question"    => chunkQuestion(objectId, data)
      case "Collection"  => chunkCollection(objectId, data)
      case "QuestionSet" => chunkQuestionSet(objectId, data)
      case _ =>
        logger.warn(s"Unknown content type: $contentType")
        List(TextChunk(s"Unknown type: $contentType", "metadata", 0))
    }
  }

  private def chunkContent(objectId: String, data: Map[String, Any]): List[TextChunk] = {
    val excludedKeys = Set("hierarchy", "children", "id", "identifier", "contentType", "_schema_version", "timestamp")
    val metadataValues = data
      .filterKeys(!excludedKeys.contains(_))
      .values
      .map {
        case seq: Seq[_] => seq.map(v => if (v != null) v.toString else "").filter(_.nonEmpty).mkString(", ")
        case value if value != null => value.toString
        case _ => ""
      }
      .filter(_.nonEmpty)

    val metadataText = truncate(metadataValues.mkString(" | "))

    if (metadataText.nonEmpty) {
      logger.debug(s"Created metadata chunk for Content:$objectId with ${metadataValues.size} fields")
      List(TextChunk(text = metadataText, sourceField = "metadata", index = 0, metadata = Map("type" -> "content_metadata")))
    } else List.empty
  }

  private def chunkQuestion(objectId: String, data: Map[String, Any]): List[TextChunk] = {
    val excludedKeys = Set("hierarchy", "children", "id", "identifier", "contentType", "_schema_version", "timestamp")
    val metadataValues = data
      .filterKeys(!excludedKeys.contains(_))
      .values
      .map {
        case seq: Seq[_] => seq.map(v => if (v != null) v.toString else "").filter(_.nonEmpty).mkString(", ")
        case value if value != null => value.toString
        case _ => ""
      }
      .filter(_.nonEmpty)

    val questionText = truncate(metadataValues.mkString(" | "))

    if (questionText.nonEmpty) {
      logger.debug(s"Created full question chunk for Question:$objectId with ${metadataValues.size} fields")
      List(TextChunk(text = questionText, sourceField = "question_full", index = 0, metadata = Map("type" -> "question")))
    } else List.empty
  }

  private def chunkCollection(objectId: String, data: Map[String, Any]): List[TextChunk] = {
    val chunks      = scala.collection.mutable.ListBuffer[TextChunk]()
    val excludedKeys = Set("hierarchy", "children", "id", "identifier", "contentType", "_schema_version", "timestamp")
    val metadataValues = data
      .filterKeys(!excludedKeys.contains(_))
      .values
      .map {
        case seq: Seq[_] => seq.map(v => if (v != null) v.toString else "").filter(_.nonEmpty).mkString(", ")
        case value if value != null => value.toString
        case _ => ""
      }
      .filter(_.nonEmpty)

    val metadataText = truncate(metadataValues.mkString(" | "))
    if (metadataText.nonEmpty) {
      chunks += TextChunk(text = metadataText, sourceField = "collection_metadata", index = 0, metadata = Map("type" -> "collection_metadata"))
      logger.debug(s"Created metadata chunk for Collection:$objectId with ${metadataValues.size} fields")
    }

    data.get("hierarchy") match {
      case Some(hierarchyData) => chunks ++= chunkHierarchyChildren(hierarchyData, objectId, chunks.size, Set())
      case None                => logger.debug(s"No hierarchy data for Collection:$objectId")
    }

    chunks.toList
  }

  private def chunkQuestionSet(objectId: String, data: Map[String, Any]): List[TextChunk] = {
    val chunks      = scala.collection.mutable.ListBuffer[TextChunk]()
    val excludedKeys = Set("hierarchy", "children", "id", "identifier", "contentType", "_schema_version", "timestamp")
    val metadataValues = data
      .filterKeys(!excludedKeys.contains(_))
      .values
      .map {
        case seq: Seq[_] => seq.map(v => if (v != null) v.toString else "").filter(_.nonEmpty).mkString(", ")
        case value if value != null => value.toString
        case _ => ""
      }
      .filter(_.nonEmpty)

    val metadataText = truncate(metadataValues.mkString(" | "))
    if (metadataText.nonEmpty) {
      chunks += TextChunk(text = metadataText, sourceField = "questionset_metadata", index = 0, metadata = Map("type" -> "questionset_metadata"))
      logger.debug(s"Created metadata chunk for QuestionSet:$objectId with ${metadataValues.size} fields")
    }

    data.get("hierarchy") match {
      case Some(hierarchyData) => chunks ++= chunkHierarchyChildren(hierarchyData, objectId, chunks.size, Set())
      case None                => logger.debug(s"No hierarchy data for QuestionSet:$objectId")
    }

    chunks.toList
  }

  private def chunkHierarchyChildren(hierarchyData: Any, parentId: String, startIndex: Int, visitedIds: Set[String]): List[TextChunk] = {
    val chunks = scala.collection.mutable.ListBuffer[TextChunk]()
    var index  = startIndex
    val maxDepth = 50

    hierarchyData match {
      case map: Map[String, Any] @unchecked =>
        map.get("children") match {
          case Some(childList: Seq[_]) =>
            childList.foreach {
              case childMap: Map[String, Any] @unchecked =>
                val childId = getSafeString(childMap, "identifier")

                if (childId.isEmpty) {
                  logger.warn(s"Child node missing identifier under parent $parentId")
                } else if (visitedIds.contains(childId)) {
                  logger.warn(s"Circular hierarchy detected: child $childId already visited (parent: $parentId)")
                } else if (visitedIds.size >= maxDepth) {
                  logger.warn(s"Max hierarchy depth ($maxDepth) exceeded from parent $parentId, skipping nested children")
                } else {
                  val childName        = getSafeString(childMap, "name")
                  val childDescription = getSafeString(childMap, "description")
                  val childText        = truncate(Seq(childName, childDescription).filter(_.nonEmpty).mkString(" | "))

                  if (childText.nonEmpty) {
                    chunks += TextChunk(
                      text        = childText,
                      sourceField = s"child_$childId",
                      index       = index,
                      metadata    = Map("parentId" -> parentId, "childId" -> childId)
                    )
                    index += 1
                  }

                  childMap.get("children") match {
                    case Some(_) =>
                      val newVisited = visitedIds + childId
                      val nested = chunkHierarchyChildren(childMap, childId, index, newVisited)
                      chunks ++= nested
                      index  += nested.length
                    case None =>
                  }
                }
              case _ =>
            }
          case _ =>
        }
      case _ =>
    }

    chunks.toList
  }
}
