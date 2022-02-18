package org.sunbird.job.publish.helpers

import com.google.gson.Gson
import org.slf4j.LoggerFactory
import org.sunbird.job.publish.core.ObjectData
import org.sunbird.job.publish.handler.{QuestionHandlerFactory, QuestionTypeHandler}
import org.sunbird.job.util.{CloudStorageUtil, FileUtils, HttpUtil, Slug}

import java.io.{BufferedWriter, File, FileWriter}

trait QuestionPdfGenerator extends ObjectTemplateGenerator {
  private[this] val logger = LoggerFactory.getLogger(classOf[QuestionPdfGenerator])
  lazy private val gson = new Gson()

  def getPdfFileUrl(objList: List[ObjectData], obj: ObjectData, templateName: String, baseUrl: String, fileNameSuffix: String)(implicit httpUtil: HttpUtil, cloudStorageUtil: CloudStorageUtil): (Option[String], Option[String]) = {
    val previewUrl: Option[String] = getPreviewFileUrl(objList, obj, templateName, fileNameSuffix)
    logger.info(s"QuestionPdfGenerator ::: preview url (Html File Url) for ${obj.identifier} is : ${previewUrl.getOrElse("")}")
    val pdfFileUrl = convertFileToPdfUrl(previewUrl, baseUrl)
    logger.info(s"QuestionPdfGenerator ::: pdf file local path for ${obj.identifier} is : ${pdfFileUrl.getOrElse("")}")
    pdfFileUrl match {
      case Some(url: String) => (uploadFileString(url, obj, fileNameSuffix), previewUrl)
      case _ => (None, previewUrl)
    }
  }

  def getPreviewFileUrl(objList: List[ObjectData], obj: ObjectData, templateName: String, fileNameSuffix: String)(implicit cloudStorageUtil: CloudStorageUtil): Option[String] = {
    val fileContent: String = getFileString(objList, obj.getString("name", ""), templateName).getOrElse("")
    val fileName: String = s"/tmp/${obj.identifier}_${getHtmlFileSuffix(fileNameSuffix)}"
    val file: Option[File] = writeFile(fileName, fileContent)
    uploadFile(file, obj)
  }

  def getFileString(objList: List[ObjectData], fileTitle: String, templateName: String,
                    customGenerator: (List[ObjectData], String, String) => Option[String] = getHtmlString): Option[String] = {
    logger.info("Generating File string for objects")
    customGenerator(objList, fileTitle, templateName)
  }

  private def getHtmlFileSuffix(fileNameSuffix: String): String = "html_" + fileNameSuffix + ".html"

  def writeFile(filename: String, content: String): Option[File] = {
    try {
      logger.info(s"Writing to file with name $filename")
      val file = new File(filename)
      val bw = new BufferedWriter(new FileWriter(file))
      bw.write(content)
      bw.close()
      Some(file)
    } catch {
      case e: Exception =>
        logger.error("Error occurred while writing to file", e)
        None
    }
  }

  def convertFileToPdfUrl(fileString: Option[String], baseUrl: String,
                          customConverter: (String, HttpUtil, String) => Option[String] = convertHtmlToPDF)(implicit httpUtil: HttpUtil): Option[String] = {
    fileString match {
      case Some(content: String) => customConverter(content, httpUtil, baseUrl)
      case _ =>
        logger.error("Error occurred while converting file, File cannot be empty")
        None
    }
  }

  def getHtmlString(questions: List[ObjectData], title: String, templateName: String): Option[String] = {
    val questionsDataMap = populateQuestionsData(questions)
    generateHtmlString(questionsDataMap, title, templateName) match {
      case "" => None
      case x: String => Some(x)
    }
  }

  //Will require index (Need to be got from question set)
  private def populateQuestionsData(questions: List[ObjectData]): Map[String, AnyRef] = {
    questions.map(question => question.dbId -> {
      val handlerOption = QuestionHandlerFactory.apply(question.metadata.get("primaryCategory").asInstanceOf[Option[String]])
      handlerOption match {
        case Some(handler: QuestionTypeHandler) =>
          Map("question" -> handler.getQuestion(question.extData),
            "answer" -> handler.getAnswers(question.extData),
            "index" -> question.metadata.getOrElse("index", 0.asInstanceOf[AnyRef]))
        case _ => Map()
      }
    }).toMap
  }

  private def generateHtmlString(questionsMap: Map[String, AnyRef], title: String, templateName: String): String = {
    val filteredQMap = questionsMap.filter(entry => entry._2.asInstanceOf[Map[String, AnyRef]].nonEmpty)
    if(filteredQMap.isEmpty) return ""
    val questionString: String = questionsMap.map(entry =>
      s"""
         |<div class='question-section'>
         |<div class='question-count'>
         |${entry._2.asInstanceOf[Map[String, AnyRef]].getOrElse("index", 0.asInstanceOf[AnyRef])}.&nbsp
         |</div>
         |${entry._2.asInstanceOf[Map[String, AnyRef]].getOrElse("question", "")}
         |</div>
            """.stripMargin
    ).reduce(_ + _)
    val answerString: String = questionsMap.map(entry =>
      s"""
         |<div class='question-section'>
         |<div class='question-count'>
         |${entry._2.asInstanceOf[Map[String, AnyRef]].getOrElse("index", 0.asInstanceOf[AnyRef])}.&nbsp
         |</div>
         |${entry._2.asInstanceOf[Map[String, AnyRef]].getOrElse("question", "")}
         |</div>
         |<div class='answer'>
         |${entry._2.asInstanceOf[Map[String, AnyRef]].getOrElse("answer", List()).asInstanceOf[List[String]].mkString(", ")}
         |</div>
            """.stripMargin
    ).reduce(_ + _)
    val velocityContext: Map[String, AnyRef] = Map("questions" -> questionString, "answers" -> answerString, "title" -> title)
    handleHtmlTemplate(templateName, velocityContext)
  }

  //TODO: Remove hardcoded print-service url
  private def convertHtmlToPDF(htmlFileUrl: String, httpUtil: HttpUtil, baseUrl: String): Option[String] = {
    val response = httpUtil.post(s"$baseUrl/v1/print/preview/generate?fileUrl=$htmlFileUrl", "")
    if (response.status == 200) {
      val responseBody = gson.fromJson(response.body, classOf[java.util.Map[String, AnyRef]])
      val result = responseBody.getOrDefault("result", new java.util.HashMap[String, AnyRef]()).asInstanceOf[java.util.Map[String, AnyRef]]
      val pdfUrl = result.getOrDefault("pdfUrl", "").asInstanceOf[String]
      logger.info("QuestionPdfGenerator ::: pdf file url generated by print service : " + pdfUrl)
      if (pdfUrl.isEmpty) None else Some(pdfUrl)
    } else if (response.status == 400) {
      logger.error("Client Error during Generate Question Set previewUrl: " + response.status)
      None
    } else {
      logger.error("Server Error during Generate Question Set previewUrl: " + response.status)
      None
    }
  }

  private def uploadFile(fileOption: Option[File], obj: ObjectData)(implicit cloudStorageUtil: CloudStorageUtil): Option[String] = {
    fileOption match {
      case Some(file: File) =>
        val folder = "questionset" + File.separator + obj.identifier
        val urlArray: Array[String] = cloudStorageUtil.uploadFile(folder, file, Some(true))
        Some(urlArray(1))
      case _ => None
    }
  }

  private def uploadFileString(fileUrl: String, obj: ObjectData, fileNameSuffix: String)(implicit cloudStorageUtil: CloudStorageUtil): Option[String] = {
    //Todo: Rename Status?
    val fileName = s"${obj.identifier}_pdf_$fileNameSuffix.pdf"
    FileUtils.copyURLToFile(obj.identifier, fileUrl, fileName) match {
      case Some(file: File) =>
        val folder = "questionset" + File.separator + Slug.makeSlug(obj.identifier, isTransliterate = true)
        val urlArray: Array[String] = cloudStorageUtil.uploadFile(folder, file, Some(true))
        Some(urlArray(1))
      case _ => logger.error("ERR_INVALID_FILE_URL", "Please Provide Valid File Url!")
        None
    }
  }

}
