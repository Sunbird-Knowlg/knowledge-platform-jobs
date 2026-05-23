package org.sunbird.job.contentembedding.strategy

import org.slf4j.LoggerFactory
import org.sunbird.job.contentembedding.domain.{ChunkingConfig, TextChunk}
import org.sunbird.job.contentembedding.service.ChunkingStrategy

/**
 * Sliding-window word chunking with overlap.
 *
 * Splits text into chunks of whitespace-separated words (no tokenizer dependency).
 * For each content type, extracts all relevant text fields into one string,
 * then slides a window of `maxWords` words with `overlapWords` words of overlap.
 *
 * Default: 512 words, 102 word overlap (~20%).
 *
 * Why overlap?  Without it, a sentence split across two chunk boundaries loses
 * its context in both chunks.  With 20% overlap, each boundary region is
 * represented in two consecutive chunks, so the embedding model always sees
 * the surrounding context.
 */
class SlidingWindowChunkingStrategy(config: ChunkingConfig) extends ChunkingStrategy {

  private[this] val logger = LoggerFactory.getLogger(classOf[SlidingWindowChunkingStrategy])

  private val windowSize = config.maxWords     // words per chunk
  private val overlap    = config.overlapWords  // words shared between consecutive chunks
  private val step       = math.max(1, windowSize - overlap)

  override def getName: String = "sliding-window"

  override def getVersion: String = "1.0"

  override def chunk(
      objectId: String,
      contentType: String,
      data: Map[String, Any]
  ): List[TextChunk] = {
    logger.debug(s"Chunking $contentType:$objectId using sliding-window (window=$windowSize, overlap=$overlap)")

    val fullText = contentType match {
      case "Content"     => extractContentText(data)
      case "Question"    => extractQuestionText(data)
      case "Collection"  => extractCollectionText(data)
      case "QuestionSet" => extractQuestionSetText(data)
      case _ =>
        logger.warn(s"Unknown content type: $contentType for $objectId")
        ""
    }

    if (fullText.isBlank) {
      logger.warn(s"No text extracted from $contentType:$objectId — skipping")
      return List.empty
    }

    val words = fullText.split("\\s+").filter(_.nonEmpty).toVector

    if (words.length <= windowSize) {
      // Short enough to fit in one chunk — no sliding needed
      logger.debug(s"$contentType:$objectId fits in single chunk (${words.length} words)")
      List(TextChunk(
        text        = words.mkString(" "),
        sourceField = "full_text",
        index       = 0,
        metadata    = Map("strategy" -> "sliding-window", "total_words" -> words.length)
      ))
    } else {
      val chunks = (0 until words.length by step)
        .map { start =>
          val end        = math.min(start + windowSize, words.length)
          val chunkWords = words.slice(start, end)
          (start, chunkWords)
        }
        .filter(_._2.nonEmpty)
        .zipWithIndex
        .map { case ((startPos, chunkWords), idx) =>
          TextChunk(
            text        = chunkWords.mkString(" "),
            sourceField = s"window_$idx",
            index       = idx,
            metadata    = Map(
              "strategy"      -> "sliding-window",
              "window_start"  -> startPos,
              "window_end"    -> (startPos + chunkWords.length),
              "word_count"    -> chunkWords.length
            )
          )
        }
        .toList

      logger.info(s"$contentType:$objectId → ${chunks.size} chunks from ${words.length} words (window=$windowSize, overlap=$overlap)")
      chunks
    }
  }

  // Concatenate all relevant fields into one text blob per content type.
  // Order matters: put most important fields first so early chunks carry more signal.

  private def extractContentText(data: Map[String, Any]): String = {
    val name        = str(data, "name")
    val description = str(data, "description")
    val keywords    = listOrStr(data, "keywords")
    val subject     = listOrStr(data, "subject")
    val body        = str(data, "body")
    Seq(name, description, keywords, subject, body).filter(_.nonEmpty).mkString(" ")
  }

  private def extractQuestionText(data: Map[String, Any]): String = {
    val name        = str(data, "name")
    val description = str(data, "description")
    val body        = str(data, "body")
    val subject     = listOrStr(data, "subject")
    // MCQ answer options — include them for better semantic coverage
    val options     = data.get("responseDeclaration") match {
      case Some(rd: Map[String, Any] @unchecked) =>
        rd.get("response1") match {
          case Some(r1: Map[String, Any] @unchecked) =>
            r1.get("mapping") match {
              case Some(mapping: List[_]) =>
                mapping.collect { case m: Map[String, Any] @unchecked => str(m, "value") }
                  .filter(_.nonEmpty).mkString(" ")
              case _ => ""
            }
          case _ => ""
        }
      case _ => ""
    }
    Seq(name, description, body, subject, options).filter(_.nonEmpty).mkString(" ")
  }

  private def extractCollectionText(data: Map[String, Any]): String = {
    val name        = str(data, "name")
    val description = str(data, "description")
    val subject     = listOrStr(data, "subject")
    val childText   = flattenHierarchyText(data.get("hierarchy"))
    Seq(name, description, subject, childText).filter(_.nonEmpty).mkString(" ")
  }

  private def extractQuestionSetText(data: Map[String, Any]): String = {
    val name        = str(data, "name")
    val description = str(data, "description")
    val childText   = flattenHierarchyText(data.get("hierarchy"))
    Seq(name, description, childText).filter(_.nonEmpty).mkString(" ")
  }

  private def flattenHierarchyText(hierarchyOpt: Option[Any]): String = {
    hierarchyOpt match {
      case Some(map: Map[String, Any] @unchecked) =>
        map.get("children") match {
          case Some(children: Seq[_]) if children != null =>
            children.collect { case c: Map[String, Any] @unchecked =>
              val childName = str(c, "name")
              val childDesc = str(c, "description")
              val nested    = flattenHierarchyText(Some(c))
              Seq(childName, childDesc, nested).filter(_.nonEmpty).mkString(" ")
            }.mkString(" ")
          case _ => ""
        }
      case _ => ""
    }
  }

  private def str(m: Map[String, Any], key: String): String =
    m.get(key).map(_.toString.trim).getOrElse("")

  private def listOrStr(m: Map[String, Any], key: String): String =
    m.get(key) match {
      case Some(seq: Seq[_]) if seq != null => seq.filter(_ != null).map(_.toString).mkString(" ")
      case Some(v) if v != null             => v.toString
      case _                                => ""
    }
}
