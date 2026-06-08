package org.sunbird.job.contentembedding.strategy

import org.slf4j.LoggerFactory
import org.sunbird.job.contentembedding.domain.{ChunkingConfig, TextChunk}
import org.sunbird.job.contentembedding.service.ChunkingStrategy

/**
 * Field-based chunking strategy — one semantically cohesive chunk per content section.
 *
 * Unlike sliding-window, this strategy does NOT split on word count. All fields from the
 * enriched metadata event are concatenated dynamically (excluding `config.excludedFields`),
 * with `mimeType` translated to a human-readable label and `creator`/`author` emitted
 * with their key names for richer semantic context.
 *
 * Chunking rules by content type:
 *  - '''Content'''     → 1 chunk: all non-excluded fields concatenated
 *  - '''Question'''    → 1 chunk: all non-excluded fields concatenated
 *  - '''Collection'''  → 1 metadata chunk (all non-excluded fields) + 1 chunk per hierarchy child (recursive)
 *  - '''QuestionSet''' → 1 metadata chunk (all non-excluded fields) + 1 chunk per hierarchy child (recursive)
 *
 * Best suited for short-to-medium metadata. Use `SlidingWindowChunkingStrategy` for
 * long documents (e.g. full article body > 512 words).
 *
 * @param config Chunking config; `maxChunkSize` (character limit) and `excludedFields` are used.
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

  private val mimeTypeLabels: Map[String, String] = Map(
    "application/pdf"                              -> "PDF document",
    "application/epub"                             -> "ePub document",
    "application/msword"                           -> "Word document",
    "application/json"                             -> "JSON content",
    "application/quiz"                             -> "Quiz",
    "application/octet-stream"                     -> "binary content",
    "application/vnd.android.package-archive"      -> "Android app",
    "application/vnd.ekstep.ecml-archive"          -> "ECML interactive content",
    "application/vnd.ekstep.html-archive"          -> "HTML content",
    "application/vnd.ekstep.h5p-archive"           -> "H5P interactive content",
    "application/vnd.ekstep.scorm-archive"          -> "Scorm content",
    "application/vnd.ekstep.content-archive"       -> "content archive",
    "application/vnd.ekstep.content-collection"    -> "content collection",
    "application/vnd.ekstep.plugin-archive"        -> "plugin archive",
    "application/vnd.sunbird.question"             -> "question",
    "application/vnd.sunbird.questionset"          -> "question set",
    "application/vnd.sunbird.assessmentitem"       -> "assessment item",
    "text/x-url"                                   -> "web URL",
    "video/mp4"                                    -> "MP4 video",
    "video/webm"                                   -> "WebM video",
    "video/ogg"                                    -> "OGG video",
    "video/avi"                                    -> "AVI video",
    "video/mpeg"                                   -> "MPEG video",
    "video/quicktime"                              -> "QuickTime video",
    "video/3gpp"                                   -> "3GPP video",
    "video/x-youtube"                              -> "YouTube video",
    "audio/mp3"                                    -> "MP3 audio",
    "audio/mp4"                                    -> "MP4 audio",
    "audio/mpeg"                                   -> "MP3 audio",
    "audio/ogg"                                    -> "OGG audio",
    "audio/webm"                                   -> "WebM audio",
    "audio/wav"                                    -> "WAV audio",
    "audio/x-wav"                                  -> "WAV audio",
    "image/jpeg"                                   -> "JPEG image",
    "image/jpg"                                    -> "JPEG image",
    "image/png"                                    -> "PNG image",
    "image/gif"                                    -> "GIF image",
    "image/bmp"                                    -> "BMP image",
    "image/tiff"                                   -> "TIFF image",
    "image/svg+xml"                                -> "SVG image"
  )

  private def renderValue(key: String, raw: Any): String = raw match {
    case seq: Seq[_] if seq != null =>
      seq.map(v => if (v != null) v.toString else "").filter(_.nonEmpty).mkString(", ")
    case value if value != null =>
      val str = value.toString
      if (key == "mimeType") mimeTypeLabels.getOrElse(str, str) else str
    case _ => ""
  }

  private def extractMetadata(data: Map[String, Any]): String = {
    val metadataValues = data
      .filterKeys(!config.excludedFields.contains(_))
      .map { case (key, value) =>
        val rendered = renderValue(key, value)
        if (rendered.isEmpty) ""
        else s"$key: $rendered"
      }
      .filter(_.nonEmpty)

    truncate(metadataValues.mkString(" | "))
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
    val metadataText = extractMetadata(data)

    if (metadataText.nonEmpty) {
      logger.debug(s"Created metadata chunk for Content:$objectId")
      List(TextChunk(text = metadataText, sourceField = "metadata", index = 0, metadata = Map("type" -> "content_metadata")))
    } else List.empty
  }

  private def chunkQuestion(objectId: String, data: Map[String, Any]): List[TextChunk] = {
    val questionText = extractMetadata(data)

    if (questionText.nonEmpty) {
      logger.debug(s"Created full question chunk for Question:$objectId")
      List(TextChunk(text = questionText, sourceField = "question_full", index = 0, metadata = Map("type" -> "question")))
    } else List.empty
  }

  private def chunkCollection(objectId: String, data: Map[String, Any]): List[TextChunk] = {
    val chunks        = scala.collection.mutable.ListBuffer[TextChunk]()
    val metadataText  = extractMetadata(data)

    if (metadataText.nonEmpty) {
      chunks += TextChunk(text = metadataText, sourceField = "collection_metadata", index = 0, metadata = Map("type" -> "collection_metadata"))
      logger.debug(s"Created metadata chunk for Collection:$objectId")
    }

    data.get("hierarchy") match {
      case Some(hierarchyData) => chunks ++= chunkHierarchyChildren(hierarchyData, objectId, chunks.size, Set(), 0)
      case None                => logger.debug(s"No hierarchy data for Collection:$objectId")
    }

    chunks.toList
  }

  private def chunkQuestionSet(objectId: String, data: Map[String, Any]): List[TextChunk] = {
    val chunks        = scala.collection.mutable.ListBuffer[TextChunk]()
    val metadataText  = extractMetadata(data)

    if (metadataText.nonEmpty) {
      chunks += TextChunk(text = metadataText, sourceField = "questionset_metadata", index = 0, metadata = Map("type" -> "questionset_metadata"))
      logger.debug(s"Created metadata chunk for QuestionSet:$objectId")
    }

    data.get("hierarchy") match {
      case Some(hierarchyData) => chunks ++= chunkHierarchyChildren(hierarchyData, objectId, chunks.size, Set(), 0)
      case None                => logger.debug(s"No hierarchy data for QuestionSet:$objectId")
    }

    chunks.toList
  }

  private def chunkHierarchyChildren(hierarchyData: Any, parentId: String, startIndex: Int, visitedIds: Set[String], depth: Int): List[TextChunk] = {
    val chunks = scala.collection.mutable.ListBuffer[TextChunk]()
    var index  = startIndex
    val maxDepth = 50

    if (depth >= maxDepth) {
      logger.warn(s"Max hierarchy depth ($maxDepth) exceeded from parent $parentId, skipping nested children")
      return chunks.toList
    }

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
                      val nested = chunkHierarchyChildren(childMap, childId, index, newVisited, depth + 1)
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
