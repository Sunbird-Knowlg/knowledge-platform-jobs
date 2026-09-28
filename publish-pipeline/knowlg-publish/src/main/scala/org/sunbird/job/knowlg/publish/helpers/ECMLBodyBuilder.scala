package org.sunbird.job.knowlg.publish.helpers

import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.sunbird.job.knowlg.task.KnowlgPublishConfig

import java.util.UUID
import scala.collection.JavaConverters._

/** Ported from fmps's ECMLBodyBuilder — builds the `body` field (MCQ quiz stage, manifest, summary) from selected items. */
object ECMLBodyBuilder {
  implicit val formats: Formats = DefaultFormats

  def buildEcmlBodyFromItems(items: List[Map[String, AnyRef]], quizTitle: String, config: KnowlgPublishConfig): String = {
    val defaultIsShuffleOption: Boolean = config.defaultIsShuffleOption
    val defaultIsPartialScore: Boolean = config.defaultIsPartialScore

    def getString(m: Map[String, AnyRef], key: String, default: String = ""): String = m.get(key).map(_.toString).getOrElse(default)
    def getInt(m: Map[String, AnyRef], key: String, default: Int = 0): Int = m.get(key).map {
      case i: java.lang.Integer => i.intValue()
      case l: java.lang.Long    => l.toInt
      case s: String            => scala.util.Try(s.toInt).getOrElse(default)
      case other                => scala.util.Try(other.toString.toInt).getOrElse(default)
    }.getOrElse(default)
    def getBool(m: Map[String, AnyRef], key: String, default: Boolean = false): Boolean = m.get(key).map(_.toString.equalsIgnoreCase("true")).getOrElse(default)
    def toStrList(v: AnyRef): List[String] = v match {
      case null                 => Nil
      case l: java.util.List[_] => l.asScala.toList.map(_.toString)
      case l: List[_]           => l.map(_.toString)
      case arr: Array[_]        => arr.map(_.toString).toList
      case s: String            => List(s)
      case _                    => Nil
    }
    def extractTitle(item: Map[String, AnyRef]): String = getString(item, "title", getString(item, "name", "")).trim

    def parseRawItemOptions(item: Map[String, AnyRef]): List[Map[String, Any]] = {
      val bodyStr = getString(item, "body")
      if (bodyStr.nonEmpty) {
        try {
          val bodyJson = parse(bodyStr)
          val options = (bodyJson \ "data" \ "data" \ "options").extract[List[Map[String, Any]]]
          options.zipWithIndex.map { case (opt, idx) =>
            val isCorrect = opt.getOrElse("isCorrect", false) match {
              case b: Boolean => b
              case s: String => s.toBoolean
              case _ => false
            }
            Map(
              "answer" -> isCorrect,
              "value" -> Map(
                "type" -> "text",
                "asset" -> "1",
                "resvalue" -> (if (isCorrect) 1 else 0),
                "resindex" -> idx
              )
            )
          }
        } catch { case _: Exception => Nil }
      } else Nil
    }

    def parseRootQuestionOptions(item: Map[String, AnyRef]): List[Map[String, Any]] = {
      val bodyStr = getString(item, "body")
      if (bodyStr.nonEmpty) {
        try {
          val bodyJson = parse(bodyStr)
          val options = (bodyJson \ "data" \ "data" \ "options").extract[List[Map[String, Any]]]
          options.zipWithIndex.map { case (opt, idx) =>
            val text = opt.getOrElse("text", "").toString
            val styledText = text.replace("<p>", "<p style='font-size:1.285em;'>")
            Map(
              "text" -> styledText,
              "image" -> opt.getOrElse("image", ""),
              "audio" -> opt.getOrElse("audio", ""),
              "audioName" -> opt.getOrElse("audioName", ""),
              "hint" -> opt.getOrElse("hint", ""),
              "isCorrect" -> opt.getOrElse("isCorrect", false),
              "$$hashKey" -> opt.getOrElse("$$hashKey", "")
            )
          }
        } catch { case _: Exception => Nil }
      } else Nil
    }

    def buildStyledRootQuestionData(item: Map[String, AnyRef]): String = {
      val bodyStr = getString(item, "body")
      if (bodyStr.nonEmpty) {
        try {
          val bodyJson = parse(bodyStr)
          val questionObj = (bodyJson \ "data" \ "data" \ "question").extract[Map[String, Any]]
          val options = parseRootQuestionOptions(item)

          val questionText = questionObj.getOrElse("text", "").toString
          val styledQuestionText = questionText.replace("<p>", "<p style='font-size:1.285em;'>")

          val styledQuestion = questionObj.updated("text", styledQuestionText)

          val questionData = Map(
            "question" -> styledQuestion,
            "options" -> options,
            "questionCount" -> 0,
            "media" -> List.empty
          )

          compact(render(Extraction.decompose(questionData)))
        } catch { case _: Exception => "" }
      } else ""
    }

    def buildConfigData(item: Map[String, AnyRef]): String = {
      val bodyStr = getString(item, "body")
      if (bodyStr.nonEmpty) {
        try {
          val bodyJson = parse(bodyStr)
          val configJson = bodyJson \ "data" \ "config"
          val metadataJson = configJson \ "metadata"

          val maxScore = ((configJson \ "max_score").extractOpt[Int] orElse (configJson \ "max_score").extractOpt[BigInt].map(_.toInt)).getOrElse(getInt(item, "max_score", 1))
          val isShuffleOption = (configJson \ "isShuffleOption").extractOpt[Boolean].getOrElse(getBool(item, "isShuffleOption", defaultIsShuffleOption))
          val isPartialScore = (configJson \ "isPartialScore").extractOpt[Boolean].getOrElse(getBool(item, "isPartialScore", defaultIsPartialScore))
          val evalUnordered = (configJson \ "evalUnordered").extractOpt[Boolean].getOrElse(getBool(item, "evalUnordered"))
          val templateType = (metadataJson \ "templateType").extractOpt[String].getOrElse(getString(item, "templateType", "Horizontal"))
          val name = (metadataJson \ "name").extractOpt[String].getOrElse(extractTitle(item))
          val title = (metadataJson \ "title").extractOpt[String].getOrElse(extractTitle(item))
          val copyright = (metadataJson \ "copyright").extractOpt[String].getOrElse(getString(item, "copyright", "Sunbird Org"))
          val qlevel = (metadataJson \ "qlevel").extractOpt[String].getOrElse(getString(item, "qlevel", "")).toUpperCase
          val category = (metadataJson \ "category").extractOpt[String].getOrElse(getString(item, "category", "MCQ"))
          val observableElement = (metadataJson \ "observableElement").extractOpt[List[String]].getOrElse(toStrList(item.getOrElse("skill", List.empty)))

          val metadataFields: List[JField] = List(
            JField("max_score", JInt(BigInt(maxScore))),
            JField("isShuffleOption", JBool(isShuffleOption)),
            JField("isPartialScore", JBool(isPartialScore)),
            JField("evalUnordered", JBool(evalUnordered)),
            JField("templateType", JString(templateType)),
            JField("name", JString(name)),
            JField("title", JString(title)),
            JField("copyright", JString(copyright)),
            JField("qlevel", JString(qlevel)),
            JField("category", JString(category)),
            JField("observableElement", Extraction.decompose(observableElement))
          )

          val configFields: List[JField] = List(
            JField("metadata", JObject(metadataFields)),
            JField("max_time", JInt(BigInt((configJson \ "max_time").extractOpt[Int].getOrElse(0)))),
            JField("max_score", JInt(BigInt(maxScore))),
            JField("partial_scoring", JBool(isPartialScore)),
            JField("layout", JString((configJson \ "layout").extractOpt[String].getOrElse("Horizontal"))),
            JField("isShuffleOption", JBool(isShuffleOption)),
            JField("questionCount", JInt(BigInt((configJson \ "questionCount").extractOpt[Int].getOrElse(1)))),
            JField("evalUnordered", JBool(evalUnordered))
          )
          compact(render(JObject(configFields)))
        } catch { case _: Exception => "" }
      } else ""
    }

    def buildQuestionBody(item: Map[String, AnyRef]): String = {
      getString(item, "body")
    }

    val stageId = UUID.randomUUID().toString
    val totalMaxScore = items.map(getInt(_, "max_score", 1)).sum
    val questionSetId = UUID.randomUUID().toString

    val questionSetDataArray = items.map { item =>
      val title = extractTitle(item)
      val parsedOptions = parseRawItemOptions(item) match {
        case Nil => List(Map("answer" -> true, "value" -> Map("type" -> "text", "asset" -> "1", "resvalue" -> 0, "resindex" -> 0)))
        case l   => l
      }
      JObject(List(
        JField("template", JString(getString(item, "template", "NA"))),
        JField("templateType", JString(getString(item, "templateType", "Horizontal"))),
        JField("copyright", JString(getString(item, "copyright", "Sunbird Org"))),
        JField("itemType", JString(getString(item, "itemType", "UNIT"))),
        JField("isPartialScore", JBool(getBool(item, "isPartialScore", defaultIsPartialScore))),
        JField("code", JString(getString(item, "code", "NA"))),
        JField("subject", JString(getString(item, "subject", "domain"))),
        JField("evalUnordered", JBool(getBool(item, "evalUnordered"))),
        JField("qlevel", JString(getString(item, "qlevel", "").toUpperCase)),
        JField("channel", JString(getString(item, "channel", ""))),
        JField("language", Extraction.decompose(toStrList(item.getOrElse("language", List.empty)))),
        JField("title", JString(title)),
        JField("type", JString(getString(item, "qType", "mcq").toLowerCase)),
        JField("editorState", JNull),
        JField("body", JString(buildQuestionBody(item))),
        JField("createdOn", JString(getString(item, "createdOn", ""))),
        JField("isShuffleOption", JBool(getBool(item, "isShuffleOption", defaultIsShuffleOption))),
        JField("appId", JString(getString(item, "appId", ""))),
        JField("options", Extraction.decompose(parsedOptions)),
        JField("observableElement", Extraction.decompose(toStrList(item.getOrElse("skill", List.empty)))),
        JField("lastUpdatedOn", JString(getString(item, "lastUpdatedOn", ""))),
        JField("identifier", JString(getString(item, "identifier", ""))),
        JField("lastStatusChangedOn", JString(getString(item, "lastStatusChangedOn", ""))),
        JField("question", JNull),
        JField("consumerId", JString(getString(item, "consumerId", ""))),
        JField("solutions", JNull),
        JField("version", JInt(getInt(item, "version", 2))),
        JField("versionKey", JString(getString(item, "versionKey", ""))),
        JField("framework", JString(getString(item, "framework", ""))),
        JField("createdBy", JString(getString(item, "createdBy", ""))),
        JField("max_score", JInt(getInt(item, "max_score", 1))),
        JField("name", JString(title)),
        JField("template_id", JString(getString(item, "template_id", getString(item, "template", "NA")))),
        JField("category", JString(getString(item, "category", "MCQ"))),
        JField("status", JString(getString(item, "status", "Live"))),
        JField("isSelected", JBool(true))
      ))
    }

    val rootQuestions = items.map { item =>
      val id = getString(item, "identifier", UUID.randomUUID().toString)
      val qType = getString(item, "qType", "mcq").toLowerCase
      val dataCdata = buildStyledRootQuestionData(item)
      val configCdata = buildConfigData(item)
      JObject(List(
        JField("id", JString(id)),
        JField("type", JString(qType)),
        JField("pluginId", JString("org.ekstep.questionunit.mcq")),
        JField("pluginVer", JString("1.3")),
        JField("templateId", JString("horizontalMCQ")),
        JField("data", JObject(List(JField("__cdata", JString(dataCdata))))),
        JField("config", JObject(List(JField("__cdata", JString(configCdata))))),
        JField("w", JInt(80)),
        JField("h", JInt(85)),
        JField("x", JInt(9)),
        JField("y", JInt(6))
      ))
    }

    val questionSetConfig = JObject(List(
      JField("title", JString(quizTitle)),
      JField("max_score", JInt(totalMaxScore)),
      JField("allow_skip", JBool(true)),
      JField("show_feedback", JBool(false)),
      JField("shuffle_questions", JBool(false)),
      JField("shuffle_options", JBool(false)),
      JField("total_items", JInt(items.size)),
      JField("btn_edit", JString("Edit"))
    ))

    val manifestMedia = List(
      JObject(List(JField("id", JString(UUID.randomUUID().toString)), JField("plugin", JString("org.ekstep.navigation")), JField("ver", JString("1.0")), JField("src", JString("/content-plugins/org.ekstep.navigation-1.0/renderer/controller/navigation_ctrl.js")), JField("type", JString("js")))),
      JObject(List(JField("id", JString(UUID.randomUUID().toString)), JField("plugin", JString("org.ekstep.navigation")), JField("ver", JString("1.0")), JField("src", JString("/content-plugins/org.ekstep.navigation-1.0/renderer/templates/navigation.html")), JField("type", JString("js")))),
      JObject(List(JField("id", JString("org.ekstep.navigation")), JField("plugin", JString("org.ekstep.navigation")), JField("ver", JString("1.0")), JField("src", JString("/content-plugins/org.ekstep.navigation-1.0/renderer/plugin.js")), JField("type", JString("plugin")))),
      JObject(List(JField("id", JString("org.ekstep.navigation_manifest")), JField("plugin", JString("org.ekstep.navigation")), JField("ver", JString("1.0")), JField("src", JString("/content-plugins/org.ekstep.navigation-1.0/manifest.json")), JField("type", JString("json")))),
      JObject(List(JField("id", JString("org.ekstep.questionset.quiz")), JField("plugin", JString("org.ekstep.questionset.quiz")), JField("ver", JString("1.0")), JField("src", JString("/content-plugins/org.ekstep.questionset.quiz-1.0/renderer/plugin.js")), JField("type", JString("plugin")))),
      JObject(List(JField("id", JString("org.ekstep.questionset.quiz_manifest")), JField("plugin", JString("org.ekstep.questionset.quiz")), JField("ver", JString("1.0")), JField("src", JString("/content-plugins/org.ekstep.questionset.quiz-1.0/manifest.json")), JField("type", JString("json")))),
      JObject(List(JField("id", JString("org.ekstep.iterator")), JField("plugin", JString("org.ekstep.iterator")), JField("ver", JString("1.0")), JField("src", JString("/content-plugins/org.ekstep.iterator-1.0/renderer/plugin.js")), JField("type", JString("plugin")))),
      JObject(List(JField("id", JString("org.ekstep.iterator_manifest")), JField("plugin", JString("org.ekstep.iterator")), JField("ver", JString("1.0")), JField("src", JString("/content-plugins/org.ekstep.iterator-1.0/manifest.json")), JField("type", JString("json")))),
      JObject(List(JField("id", JString(UUID.randomUUID().toString)), JField("plugin", JString("org.ekstep.questionset")), JField("ver", JString("1.0")), JField("src", JString("/content-plugins/org.ekstep.questionset-1.0/renderer/utils/telemetry_logger.js")), JField("type", JString("js")))),
      JObject(List(JField("id", JString(UUID.randomUUID().toString)), JField("plugin", JString("org.ekstep.questionset")), JField("ver", JString("1.0")), JField("src", JString("/content-plugins/org.ekstep.questionset-1.0/renderer/utils/html_audio_plugin.js")), JField("type", JString("js")))),
      JObject(List(JField("id", JString(UUID.randomUUID().toString)), JField("plugin", JString("org.ekstep.questionset")), JField("ver", JString("1.0")), JField("src", JString("/content-plugins/org.ekstep.questionset-1.0/renderer/utils/qs_feedback_popup.js")), JField("type", JString("js")))),
      JObject(List(JField("id", JString("org.ekstep.questionset")), JField("plugin", JString("org.ekstep.questionset")), JField("ver", JString("1.0")), JField("src", JString("/content-plugins/org.ekstep.questionset-1.0/renderer/plugin.js")), JField("type", JString("plugin")))),
      JObject(List(JField("id", JString("org.ekstep.questionset_manifest")), JField("plugin", JString("org.ekstep.questionset")), JField("ver", JString("1.0")), JField("src", JString("/content-plugins/org.ekstep.questionset-1.0/manifest.json")), JField("type", JString("json")))),
      JObject(List(JField("id", JString("org.ekstep.questionunit.renderer.audioicon")), JField("plugin", JString("org.ekstep.questionunit")), JField("ver", JString("1.2")), JField("src", JString("/content-plugins/org.ekstep.questionunit-1.2/renderer/assets/audio-icon.png")), JField("type", JString("image")))),
      JObject(List(JField("id", JString("org.ekstep.questionunit.renderer.downarrow")), JField("plugin", JString("org.ekstep.questionunit")), JField("ver", JString("1.2")), JField("src", JString("/content-plugins/org.ekstep.questionunit-1.2/renderer/assets/down_arrow.png")), JField("type", JString("image")))),
      JObject(List(JField("id", JString("org.ekstep.questionunit.renderer.zoom")), JField("plugin", JString("org.ekstep.questionunit")), JField("ver", JString("1.2")), JField("src", JString("/content-plugins/org.ekstep.questionunit-1.2/renderer/assets/zoom.png")), JField("type", JString("image")))),
      JObject(List(JField("id", JString("org.ekstep.questionunit.renderer.audio-icon1")), JField("plugin", JString("org.ekstep.questionunit")), JField("ver", JString("1.2")), JField("src", JString("/content-plugins/org.ekstep.questionunit-1.2/renderer/assets/audio-icon1.png")), JField("type", JString("image")))),
      JObject(List(JField("id", JString(UUID.randomUUID().toString)), JField("plugin", JString("org.ekstep.questionunit")), JField("ver", JString("1.2")), JField("src", JString("/content-plugins/org.ekstep.questionunit-1.2/renderer/components/js/components.js")), JField("type", JString("js")))),
      JObject(List(JField("id", JString(UUID.randomUUID().toString)), JField("plugin", JString("org.ekstep.questionunit")), JField("ver", JString("1.2")), JField("src", JString("/content-plugins/org.ekstep.questionunit-1.2/renderer/components/css/components.css")), JField("type", JString("css")))),
      JObject(List(JField("id", JString("org.ekstep.questionunit")), JField("plugin", JString("org.ekstep.questionunit")), JField("ver", JString("1.2")), JField("src", JString("/content-plugins/org.ekstep.questionunit-1.2/renderer/plugin.js")), JField("type", JString("plugin")))),
      JObject(List(JField("id", JString("org.ekstep.questionunit_manifest")), JField("plugin", JString("org.ekstep.questionunit")), JField("ver", JString("1.2")), JField("src", JString("/content-plugins/org.ekstep.questionunit-1.2/manifest.json")), JField("type", JString("json")))),
      JObject(List(JField("id", JString(UUID.randomUUID().toString)), JField("plugin", JString("org.ekstep.questionunit.mcq")), JField("ver", JString("1.3")), JField("src", JString("/content-plugins/org.ekstep.questionunit.mcq-1.3/renderer/styles/style.css")), JField("type", JString("css")))),
      JObject(List(JField("id", JString(UUID.randomUUID().toString)), JField("plugin", JString("org.ekstep.questionunit.mcq")), JField("ver", JString("1.3")), JField("src", JString("/content-plugins/org.ekstep.questionunit.mcq-1.3/renderer/styles/horizontal_and_vertical.css")), JField("type", JString("css")))),
      JObject(List(JField("id", JString(UUID.randomUUID().toString)), JField("plugin", JString("org.ekstep.questionunit.mcq")), JField("ver", JString("1.3")), JField("src", JString("/content-plugins/org.ekstep.questionunit.mcq-1.3/renderer/template/mcq-layouts.js")), JField("type", JString("js")))),
      JObject(List(JField("id", JString(UUID.randomUUID().toString)), JField("plugin", JString("org.ekstep.questionunit.mcq")), JField("ver", JString("1.3")), JField("src", JString("/content-plugins/org.ekstep.questionunit.mcq-1.3/renderer/template/template_controller.js")), JField("type", JString("js")))),
      JObject(List(JField("id", JString(UUID.randomUUID().toString)), JField("plugin", JString("org.ekstep.questionunit.mcq")), JField("ver", JString("1.3")), JField("src", JString("/content-plugins/org.ekstep.questionunit.mcq-1.3/renderer/template/question-component.js")), JField("type", JString("js")))),
      JObject(List(JField("id", JString(UUID.randomUUID().toString)), JField("plugin", JString("org.ekstep.questionunit.mcq")), JField("ver", JString("1.3")), JField("src", JString("/content-plugins/org.ekstep.questionunit.mcq-1.3/renderer/assets/tick_icon.png")), JField("type", JString("image")))),
      JObject(List(JField("id", JString(UUID.randomUUID().toString)), JField("plugin", JString("org.ekstep.questionunit.mcq")), JField("ver", JString("1.3")), JField("src", JString("/content-plugins/org.ekstep.questionunit.mcq-1.3/renderer/assets/audio-icon2.png")), JField("type", JString("image")))),
      JObject(List(JField("id", JString(UUID.randomUUID().toString)), JField("plugin", JString("org.ekstep.questionunit.mcq")), JField("ver", JString("1.3")), JField("src", JString("/content-plugins/org.ekstep.questionunit.mcq-1.3/renderer/assets/music-blue.png")), JField("type", JString("image")))),
      JObject(List(JField("id", JString("org.ekstep.questionunit.mcq")), JField("plugin", JString("org.ekstep.questionunit.mcq")), JField("ver", JString("1.3")), JField("src", JString("/content-plugins/org.ekstep.questionunit.mcq-1.3/renderer/plugin.js")), JField("type", JString("plugin")))),
      JObject(List(JField("id", JString("org.ekstep.questionunit.mcq_manifest")), JField("plugin", JString("org.ekstep.questionunit.mcq")), JField("ver", JString("1.3")), JField("src", JString("/content-plugins/org.ekstep.questionunit.mcq-1.3/manifest.json")), JField("type", JString("json")))),
      JObject(List(JField("id", JString("org.ekstep.summary_template_js")), JField("plugin", JString("org.ekstep.summary")), JField("src", JString("/content-plugins/org.ekstep.summary-1.0/renderer/summary-template.js")), JField("type", JString("js")), JField("ver", JString("1.0")))),
      JObject(List(JField("id", JString("org.ekstep.summary_template_css")), JField("plugin", JString("org.ekstep.summary")), JField("src", JString("/content-plugins/org.ekstep.summary-1.0/renderer/style.css")), JField("type", JString("css")), JField("ver", JString("1.0")))),
      JObject(List(JField("id", JString("org.ekstep.summary")), JField("plugin", JString("org.ekstep.summary")), JField("src", JString("/content-plugins/org.ekstep.summary-1.0/renderer/plugin.js")), JField("type", JString("plugin")), JField("ver", JString("1.0")))),
      JObject(List(JField("id", JString("org.ekstep.summary_manifest")), JField("plugin", JString("org.ekstep.summary")), JField("src", JString("/content-plugins/org.ekstep.summary-1.0/manifest.json")), JField("type", JString("json")), JField("ver", JString("1.0")))),
      JObject(List(JField("assetId", JString("summaryImage")), JField("id", JString("org.ekstep.summary_summaryImage")), JField("preload", JBool(true)), JField("src", JString("/content-plugins/org.ekstep.summary-1.0/assets/summary-icon.jpg")), JField("type", JString("image")))),
      JObject(List(JField("id", JString("QuizImage")), JField("src", JString("/content-plugins/org.ekstep.questionset-1.0/editor/assets/quizimage.png")), JField("assetId", JString("QuizImage")), JField("type", JString("image")), JField("preload", JBool(true))))
    )

    val pluginManifest = JObject(List(JField("plugin", JArray(List(
      JObject(List(JField("id", JString("org.ekstep.navigation")), JField("ver", JString("1.0")), JField("type", JString("plugin")), JField("depends", JString("")))),
      JObject(List(JField("id", JString("org.ekstep.questionset.quiz")), JField("ver", JString("1.0")), JField("type", JString("plugin")), JField("depends", JString("")))),
      JObject(List(JField("id", JString("org.ekstep.iterator")), JField("ver", JString("1.0")), JField("type", JString("plugin")), JField("depends", JString("")))),
      JObject(List(JField("id", JString("org.ekstep.questionset")), JField("ver", JString("1.0")), JField("type", JString("plugin")), JField("depends", JString("org.ekstep.questionset.quiz,org.ekstep.iterator")))),
      JObject(List(JField("id", JString("org.ekstep.questionunit")), JField("ver", JString("1.2")), JField("type", JString("plugin")), JField("depends", JString("")))),
      JObject(List(JField("id", JString("org.ekstep.questionunit.mcq")), JField("ver", JString("1.3")), JField("type", JString("plugin")), JField("depends", JString("org.ekstep.questionunit")))),
      JObject(List(JField("depends", JString("")), JField("id", JString("org.ekstep.summary")), JField("type", JString("plugin")), JField("ver", JString("1.0"))))
    )))))

    val mainStage = JObject(List(
      JField("x", JInt(0)),
      JField("y", JInt(0)),
      JField("w", JInt(100)),
      JField("h", JInt(100)),
      JField("id", JString(stageId)),
      JField("rotate", JNull),
      JField("config", JObject(List(JField("__cdata", JString(compact(render(JObject(List(
        JField("opacity", JInt(100)), JField("strokeWidth", JInt(1)), JField("stroke", JString("rgba(255, 255, 255, 0)")), JField("autoplay", JBool(false)), JField("visible", JBool(true)), JField("color", JString("#FFFFFF")), JField("genieControls", JBool(false)), JField("instructions", JString(""))
      ))))))))),
      JField("param", JArray(List(JObject(List(JField("name", JString("next")), JField("value", JString("summary_stage_id"))))))),
      JField("manifest", JObject(List(JField("media", JArray(List()))))),
      JField("org.ekstep.questionset", JArray(List(JObject(List(
        JField("x", JInt(9)), JField("y", JInt(6)), JField("w", JInt(80)), JField("h", JInt(85)), JField("rotate", JInt(0)), JField("z-index", JInt(0)), JField("id", JString(questionSetId)),
        JField("data", JObject(List(JField("__cdata", JString(compact(render(JArray(questionSetDataArray)))))))),
        JField("config", JObject(List(JField("__cdata", JString(compact(render(questionSetConfig))))))),
        JField("org.ekstep.question", JArray(rootQuestions))
      )))))
    ))

    val summaryStage = JObject(List(
      JField("x", JInt(0)), JField("y", JInt(0)), JField("w", JInt(100)), JField("h", JInt(100)),
      JField("rotate", JNull),
      JField("config", JObject(List(JField("__cdata", JString(compact(render(JObject(List(
        JField("opacity", JInt(100)), JField("strokeWidth", JInt(1)), JField("stroke", JString("rgba(255, 255, 255, 0)")), JField("autoplay", JBool(false)), JField("visible", JBool(true)), JField("color", JString("#FFFFFF")), JField("genieControls", JBool(false)), JField("instructions", JString(""))
      ))))))))),
      JField("id", JString("summary_stage_id")),
      JField("manifest", JObject(List(JField("media", JArray(List(JObject(List(JField("assetId", JString("summaryImage")))))))))),
      JField("org.ekstep.summary", JArray(List(JObject(List(
        JField("config", JObject(List(JField("__cdata", JString(compact(render(JObject(List(
          JField("opacity", JInt(100)), JField("strokeWidth", JInt(1)), JField("stroke", JString("rgba(255, 255, 255, 0)")), JField("autoplay", JBool(false)), JField("visible", JBool(true))
        ))))))))),
        JField("id", JString("summary_plugin_id")),
        JField("rotate", JInt(0)),
        JField("x", JDouble(6.69)), JField("y", JDouble(-27.9)), JField("w", JDouble(77.45)), JField("h", JDouble(125.53)), JField("z-index", JInt(0))
      )))))
    ))

    val themeObj = JObject(List(
      JField("id", JString("theme")),
      JField("version", JString("1.0")),
      JField("startStage", JString(stageId)),
      JField("stage", JArray(List(mainStage, summaryStage))),
      JField("manifest", JObject(List(JField("media", JArray(manifestMedia))))),
      JField("plugin-manifest", pluginManifest),
      JField("compatibilityVersion", JInt(2))
    ))

    val body = JObject(List(JField("theme", themeObj)))
    compact(render(body))
  }

}
