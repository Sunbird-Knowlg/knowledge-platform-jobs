package org.sunbird.job.contentembedding.strategy

import org.slf4j.LoggerFactory
import org.sunbird.job.contentembedding.domain.{ChunkingConfig, TextChunk}
import org.sunbird.job.contentembedding.service.ChunkingStrategy

class SemanticChunkingStrategy(config: ChunkingConfig = ChunkingConfig("semantic")) extends ChunkingStrategy {

  private[this] val logger = LoggerFactory.getLogger(classOf[SemanticChunkingStrategy])

  override def getName: String = "semantic"

  override def getVersion: String = "1.0"

  private def truncate(text: String): String =
    if (text.length > config.maxChunkSize) text.take(config.maxChunkSize) else text

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
    val name        = data.get("name").map(_.toString).getOrElse("")
    val description = data.get("description").map(_.toString).getOrElse("")
    val keywords    = data.get("keywords").map {
      case list: List[_] => list.map(_.toString).mkString(", ")
      case str: String   => str
      case _             => ""
    }.getOrElse("")
    val subject     = data.get("subject").map {
      case list: List[_] => list.map(_.toString).mkString(", ")
      case str: String   => str
      case _             => ""
    }.getOrElse("")

    val metadataText = truncate(Seq(name, description, keywords, subject).filter(_.nonEmpty).mkString(" | "))

    if (metadataText.nonEmpty) {
      logger.debug(s"Created metadata chunk for Content:$objectId")
      List(TextChunk(text = metadataText, sourceField = "metadata", index = 0, metadata = Map("type" -> "content_metadata")))
    } else List.empty
  }

  private def chunkQuestion(objectId: String, data: Map[String, Any]): List[TextChunk] = {
    val name        = data.get("name").map(_.toString).getOrElse("")
    val description = data.get("description").map(_.toString).getOrElse("")
    val body        = data.get("body").map(_.toString).getOrElse("")
    val subject     = data.get("subject").map {
      case list: List[_] => list.map(_.toString).mkString(", ")
      case str: String   => str
      case _             => ""
    }.getOrElse("")

    val questionText = truncate(Seq(name, description, body, subject).filter(_.nonEmpty).mkString(" | "))

    if (questionText.nonEmpty) {
      logger.debug(s"Created full question chunk for Question:$objectId")
      List(TextChunk(text = questionText, sourceField = "question_full", index = 0, metadata = Map("type" -> "question")))
    } else List.empty
  }

  private def chunkCollection(objectId: String, data: Map[String, Any]): List[TextChunk] = {
    val chunks      = scala.collection.mutable.ListBuffer[TextChunk]()
    val name        = data.get("name").map(_.toString).getOrElse("")
    val description = data.get("description").map(_.toString).getOrElse("")
    val subject     = data.get("subject").map {
      case list: List[_] => list.map(_.toString).mkString(", ")
      case str: String   => str
      case _             => ""
    }.getOrElse("")

    val metadataText = truncate(Seq(name, description, subject).filter(_.nonEmpty).mkString(" | "))
    if (metadataText.nonEmpty)
      chunks += TextChunk(text = metadataText, sourceField = "collection_metadata", index = 0, metadata = Map("type" -> "collection_metadata"))

    data.get("hierarchy") match {
      case Some(hierarchyData) => chunks ++= chunkHierarchyChildren(hierarchyData, objectId, chunks.size)
      case None                => logger.debug(s"No hierarchy data for Collection:$objectId")
    }

    chunks.toList
  }

  private def chunkQuestionSet(objectId: String, data: Map[String, Any]): List[TextChunk] = {
    val chunks      = scala.collection.mutable.ListBuffer[TextChunk]()
    val name        = data.get("name").map(_.toString).getOrElse("")
    val description = data.get("description").map(_.toString).getOrElse("")

    val metadataText = truncate(Seq(name, description).filter(_.nonEmpty).mkString(" | "))
    if (metadataText.nonEmpty)
      chunks += TextChunk(text = metadataText, sourceField = "questionset_metadata", index = 0, metadata = Map("type" -> "questionset_metadata"))

    data.get("hierarchy") match {
      case Some(hierarchyData) => chunks ++= chunkHierarchyChildren(hierarchyData, objectId, chunks.size)
      case None                => logger.debug(s"No hierarchy data for QuestionSet:$objectId")
    }

    chunks.toList
  }

  private def chunkHierarchyChildren(hierarchyData: Any, parentId: String, startIndex: Int): List[TextChunk] = {
    val chunks = scala.collection.mutable.ListBuffer[TextChunk]()
    var index  = startIndex

    hierarchyData match {
      case map: Map[String, Any] @unchecked =>
        map.get("children") match {
          case Some(childList: List[_]) =>
            childList.foreach {
              case childMap: Map[String, Any] @unchecked =>
                val childId          = childMap.get("identifier").map(_.toString).getOrElse("")
                val childName        = childMap.get("name").map(_.toString).getOrElse("")
                val childDescription = childMap.get("description").map(_.toString).getOrElse("")
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
                    val nested = chunkHierarchyChildren(childMap, childId, index)
                    chunks ++= nested
                    index  += nested.length
                  case None =>
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
