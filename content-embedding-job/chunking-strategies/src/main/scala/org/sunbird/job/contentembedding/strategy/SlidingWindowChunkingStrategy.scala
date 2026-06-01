package org.sunbird.job.contentembedding.strategy

import org.slf4j.LoggerFactory
import org.sunbird.job.contentembedding.domain.{ChunkingConfig, TextChunk}
import org.sunbird.job.contentembedding.service.ChunkingStrategy

/**
 * Sliding-window word chunking with overlap.
 *
 * Concatenates all non-excluded fields from the enriched metadata event (same field
 * policy as [[SemanticChunkingStrategy]]) into one text blob, then slides a window of
 * `maxWords` words with `overlapWords` words of overlap.
 *
 * `mimeType` is translated to a human-readable label; `creator` and `author` are emitted
 * with their key names. Hierarchy children are flattened and appended.
 *
 * Default: 512 words per window, 102 word overlap (~20%).
 *
 * Why overlap? Without it a sentence split across two chunk boundaries loses
 * context in both. With 20% overlap each boundary region is represented in two
 * consecutive chunks so the embedding model sees the surrounding context.
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

    val fullText = extractAllFields(data)

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
      val s = value.toString
      if (key == "mimeType") mimeTypeLabels.getOrElse(s, s) else s
    case _ => ""
  }

  // All fields from data minus excluded — same policy as SemanticChunkingStrategy.
  // hierarchy children are flattened separately and appended.
  private def extractAllFields(data: Map[String, Any]): String = {
    val metaParts = data
      .filterKeys(!config.excludedFields.contains(_))
      .map { case (key, value) =>
        val rendered = renderValue(key, value)
        if (rendered.isEmpty) ""
        else s"$key: $rendered"
      }
      .filter(_.nonEmpty)
      .mkString(" ")

    val hierarchyText = flattenHierarchyText(data.get("hierarchy"))
    Seq(metaParts, hierarchyText).filter(_.nonEmpty).mkString(" ")
  }

  private def flattenHierarchyText(hierarchyOpt: Option[Any]): String = {
    hierarchyOpt match {
      case Some(map: Map[String, Any] @unchecked) =>
        map.get("children") match {
          case Some(children: Seq[_]) if children != null =>
            children.collect { case c: Map[String, Any] @unchecked =>
              val parts = Seq(
                c.get("name").map(_.toString).getOrElse(""),
                c.get("description").map(_.toString).getOrElse(""),
                flattenHierarchyText(Some(c))
              ).filter(_.nonEmpty)
              parts.mkString(" ")
            }.mkString(" ")
          case _ => ""
        }
      case _ => ""
    }
  }
}
