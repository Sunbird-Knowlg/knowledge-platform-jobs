package org.sunbird.publish.helpers

import java.io.{BufferedWriter, File, FileWriter, IOException}
import java.net.URL

import com.google.gson.Gson
import org.apache.commons.io.FileUtils
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.sunbird.job.util.HttpUtil
import org.sunbird.publish.core.{ObjectData, Slug}
import org.sunbird.publish.handler.{QuestionHandlerFactory, QuestionTypeHandler}
import org.sunbird.publish.util.CloudStorageUtil


trait QuestionPdfGenerator extends ObjectTemplateGenerator {
    private[this] val logger = LoggerFactory.getLogger(classOf[QuestionPdfGenerator])
    lazy private val gson = new Gson()


    //    Need to pass config instead of template name, print service url, itemset generate boolean etc
    def getPdfFileUrl(objList: List[ObjectData], obj: ObjectData, templateName: String)(implicit httpUtil: HttpUtil, cloudStorageUtil: CloudStorageUtil): (Option[String], Option[String] ) = {
        val previewUrl: Option[String] = getPreviewFileUrl(objList, obj, templateName)
        val pdfFileUrl = convertFileToPdfUrl(previewUrl)
        pdfFileUrl match {
            case Some(url: String) => (uploadFileString(url, obj), previewUrl)
            case _ => (None, previewUrl)
        }
    }

    //    //Need to pass config instead of template name
    def getPreviewFileUrl(objList: List[ObjectData], obj: ObjectData, templateName: String)(implicit cloudStorageUtil: CloudStorageUtil): Option[String] = {
        val fileContent: String = getFileString(objList, obj.metadata.getOrElse("name", "").asInstanceOf[String], templateName).getOrElse("")
        val fileName: String = s"/tmp/${obj.dbId}_${getHtmlFileSuffix()}"
        val file: Option[File] = writeFile(fileName, fileContent)
        println("html file path ::: "+file.get.getAbsolutePath)
        uploadFile(file, obj)
    }

    def getFileString(objList: List[ObjectData], fileTitle: String, templateName: String,
                      customGenerator: (List[ObjectData], String, String) => Option[String] = getHtmlString): Option[String] = {
        logger.info("Generating File string for objects")
        customGenerator(objList, fileTitle, templateName)
    }

    private def getHtmlFileSuffix(): String = "html_" + System.currentTimeMillis + ".html"

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

    def convertFileToPdfUrl(fileString: Option[String],
                            customConverter: (String, HttpUtil) => Option[String] = convertHtmlToPDF)(implicit httpUtil: HttpUtil): Option[String] = {
        fileString match {
            case Some(content: String) => customConverter(content, httpUtil)
            case _ =>
                logger.error("Error occurred while converting file, File cannot be empty")
                None
        }
    }

    def getHtmlString(questions: List[ObjectData], title: String, templateName: String): Option[String] = {
        val questionsDataMap = populateQuestionsData(questions)
        //val sortedQuestionsData = mutable.ListMap(questionsDataMap.toSeq.sortBy(_._2.asInstanceOf[Map[String, AnyRef]].getOrElse("index", 0).asInstanceOf[Int]): _*)
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
        println("title :::: "+title)
        if (questionsMap.isEmpty) return ""
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
               |${entry._2.asInstanceOf[Map[String, AnyRef]].getOrElse("index",0.asInstanceOf[AnyRef])}.&nbsp
               |</div>
               |${entry._2.asInstanceOf[Map[String, AnyRef]].getOrElse("question", "")}
               |</div>
               |<div class='answer'>
               |${entry._2.asInstanceOf[Map[String, AnyRef]].getOrElse("answer", "").asInstanceOf[List[String]].mkString(", ")}
               |</div>
            """.stripMargin
        ).reduce(_ + _)
        val velocityContext: Map[String, AnyRef] = Map("questions" -> questionString, "answers" -> answerString, "title" -> title)
        handleHtmlTemplate(templateName, velocityContext)
    }

    private def convertHtmlToPDF(htmlFileUrl: String, httpUtil: HttpUtil): Option[String] = {
        //Get  value from config
        val response = httpUtil.post(s"http://11.2.2.4:5001/v1/print/preview/generate?fileUrl=$htmlFileUrl", "")
        if (response.status == 200) {
            val responseBody = gson.fromJson(response.body, classOf[java.util.Map[String, AnyRef]])
            val result = responseBody.getOrDefault("result", new java.util.HashMap[String, AnyRef]()).asInstanceOf[java.util.Map[String, AnyRef]]
            val pdfUrl = result.getOrDefault("pdfUrl", "").asInstanceOf[String]
            println("pdf file path : "+pdfUrl)
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
            case Some(file: File) => {
                val folder = "questionset" + "/" + obj.dbId
                val urlArray: Array[String] = cloudStorageUtil.uploadFile(folder, file, Some(true))
                Some(urlArray(1))
            }
            case _ => None
        }
    }

    private def uploadFileString(fileUrl: String, obj: ObjectData)(implicit cloudStorageUtil: CloudStorageUtil): Option[String] = {
        //Todo: Rename Status?
        copyURLToFile(obj.dbId, fileUrl, "questionset") match {
            case Some(file: File) => {
                val folder = s"content/${Slug.makeSlug(obj.dbId, true)}/"
                val urlArray: Array[String] = cloudStorageUtil.uploadFile(folder, file, Some(true))
                Some(urlArray(1))
            }
            case _ => logger.error("ERR_INVALID_FILE_URL", "Please Provide Valid File Url!")
                None
        }
    }

    def copyURLToFile(objectId: String, fileUrl: String, suffix: String): Option[File] = try {
        val fileName = getBasePath(objectId) + "/" + suffix
        val file = new File(fileName)
        FileUtils.copyURLToFile(new URL(fileUrl), file)
        Some(file)
    } catch {
        case e: IOException => logger.error("ERR_INVALID_FILE_URL", "Please Provide Valid File Url!")
            None
    }

    def getBasePath(objectId: String): String = {
        if (!StringUtils.isBlank(objectId))
            s"/tmp/${System.currentTimeMillis}_temp/$objectId"
        else ""
    }

}
