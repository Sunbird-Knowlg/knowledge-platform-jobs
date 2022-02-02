package org.sunbird.job.qrimagegenerator.functions

import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.exception.InvalidEventException
import org.sunbird.job.qrimagegenerator.domain.Event
import org.sunbird.job.qrimagegenerator.task.QRCodeImageGeneratorConfig
import org.sunbird.job.qrimagegenerator.util.{QRCodeImageGeneratorUtil, ZipEditorUtil}
import org.sunbird.job.util.{CassandraUtil, CloudStorageUtil, FileUtils}
import org.sunbird.job.{BaseProcessFunction, Metrics}

import java.io.File
import java.util
import scala.collection.JavaConversions._

case class DialCodes(data: String, text: String, id: String, location: Option[String])

case class StorageConfig(container: String, path: String, fileName: String)

case class Config(errorCorrectionLevel: Option[String] = Option(""), pixelsPerBlock: Option[Int] = Option(0), qrCodeMargin: Option[Int] = Option(0),
                  textFontName: Option[String] = Option(""), textFontSize: Option[Int] = Option(0), textCharacterSpacing: Option[Double] = Option(0),
                  imageFormat: Option[String] = Option("png"), colourModel: Option[String] = Option(""), imageBorderSize: Option[Int] = Option(0),
                  qrCodeMarginBottom: Option[Int] = Option(0), imageMargin: Option[Int] = Option(0))

case class QRCodeImageGenerator(data: util.List[String], errorCorrectionLevel: String, pixelsPerBlock: Int, qrCodeMargin: Int, text: util.List[String], textFontName: String, textFontSize: Int, textCharacterSpacing: Double, imageBorderSize: Int, colorModel: String, fileName: util.List[String], fileFormat: String, qrCodeMarginBottom: Int, imageMargin: Int, tempFilePath: String)

class QRCodeImageGeneratorFunction(config: QRCodeImageGeneratorConfig,
                                   @transient var cassandraUtil: CassandraUtil = null,
                                   @transient var cloudStorageUtil: CloudStorageUtil = null)
                                  (implicit val stringTypeInfo: TypeInformation[String])
  extends BaseProcessFunction[Event, String](config) {

  private val LOGGER = LoggerFactory.getLogger(classOf[QRCodeImageGeneratorFunction])

  var qRCodeImageGeneratorUtil: QRCodeImageGeneratorUtil = _

  override def metricsList(): List[String] = {
    List(config.totalEventsCount, config.successEventCount, config.failedEventCount, config.skippedEventCount, config.dbFailureEventCount, config.dbHitEventCount,
      config.cloudDbHitCount, config.cloudDbFailCount)
  }

  override def open(parameters: Configuration): Unit = {
    cassandraUtil = new CassandraUtil(config.cassandraHost, config.cassandraPort)
    cloudStorageUtil = new CloudStorageUtil(config)
    qRCodeImageGeneratorUtil = new QRCodeImageGeneratorUtil(config, cassandraUtil, cloudStorageUtil)
    super.open(parameters)
  }

  override def close(): Unit = {
    cassandraUtil.close()
    super.close()
  }

  @throws(classOf[InvalidEventException])
  override def processElement(event: Event,
                              context: ProcessFunction[Event, String]#Context,
                              metrics: Metrics): Unit = {
    metrics.incCounter(config.totalEventsCount)

    val availableImages = new util.ArrayList[File]()
    var zipFile: File = null

    try {
      LOGGER.info("QRCodeImageGeneratorService:processMessage: Processing request for processId : " + event.processId + " and objectId: " + event.objectId)
      LOGGER.info("QRCodeImageGeneratorService:processMessage: Starting message processing at " + System.currentTimeMillis());
      println("QRCodeImageGeneratorService:processMessage: Processing request for processId : " + event.processId + " and objectId: " + event.objectId)
      if (event.isValid(config)) {
        println("ValidDataEvent:")

        val tempFilePath = config.lpTempFileLocation
        val dataList = new util.ArrayList[String]
        val textList = new util.ArrayList[String]
        val fileNameList = new util.ArrayList[String]

        val dialcodesWloc = event.dialCodes.filter(f => !StringUtils.equals(f.getOrElse("location", "").asInstanceOf[String], ""))
          .foreach { dialcode =>
            try {
              val fileName = dialcode.getOrElse("id", "")
              val destPath = s"""$tempFilePath${File.separator}$fileName.${event.imageFormat}"""
              //                          val fileToSave = new File(tempFilePath + File.separator + fileName + "." + event.imageFormat)
              //                          LOGGER.info("QRCodeImageGeneratorService:processMessage: creating file - " + fileToSave.getAbsolutePath())
              //                          println("QRCodeImageGeneratorService:processMessage: creating file - " + fileToSave.getAbsolutePath())
              //                          fileToSave.createNewFile()
              //                          LOGGER.info("QRCodeImageGeneratorService:processMessage: created file - " + fileToSave.getAbsolutePath)
              //                          println("QRCodeImageGeneratorService:processMessage: created file - " + fileToSave.getAbsolutePath)
              val downloadUrl = dialcode("location").asInstanceOf[String]
              val file: File = FileUtils.downloadFile(downloadUrl, destPath)
              LOGGER.info("QRCodeImageGeneratorService:processMessage: created file - " + file.getAbsolutePath)
              println("QRCodeImageGeneratorService:processMessage: created file - " + file.getAbsolutePath)
              metrics.incCounter(config.cloudDbHitCount)
              availableImages.add(file)
            } catch {
              case e: Exception =>
                metrics.incCounter(config.cloudDbFailCount)
                throw new InvalidEventException(e.getMessage, Map("partition" -> event.partition, "offset" -> event.offset), e)
            }
          }
        println("availableImages after W/0 Loc: " + availableImages)
        event.dialCodes.map { dialcode =>
          dataList.add(dialcode("data").asInstanceOf[String])
          textList.add(dialcode("text").asInstanceOf[String])
          fileNameList.add(dialcode("id").asInstanceOf[String])
        }

        val qrGenRequest: QRCodeImageGenerator = getQRCodeGenerationRequest(event.imageConfig, dataList, textList, fileNameList)
        println("qrGenRequest: " + qrGenRequest)
        val generatedImages = qRCodeImageGeneratorUtil.createQRImages(qrGenRequest, config, event.storageContainer, event.storagePath, metrics)

        if (!StringUtils.isBlank(event.processId)) {
          var zipFileName = event.storageFileName
          LOGGER.info("QRCodeImageGeneratorService:processMessage: Generating zip for QR codes with processId " + event.processId)
          if (StringUtils.isBlank(zipFileName)) zipFileName = event.processId
          availableImages.addAll(generatedImages)
          zipFile = ZipEditorUtil.zipFiles(availableImages, zipFileName, tempFilePath)
          val zipDownloadUrl = cloudStorageUtil.uploadFile(event.storagePath, zipFile, Some(false), container = event.storageContainer)
          metrics.incCounter(config.cloudDbHitCount)
          qRCodeImageGeneratorUtil.updateCassandra(config.cassandraDialCodeBatchTable, 2, zipDownloadUrl(1), "processid", event.processId, metrics)
        }
        else {
          LOGGER.info("QRCodeImageGeneratorService:processMessage: Skipping zip creation due to missing processId.")
        }
        LOGGER.info("QRCodeImageGeneratorService:processMessage: Message processed successfully at " + System.currentTimeMillis)
      } else {
        LOGGER.info("QRCodeImageGeneratorService: Eid other than BE_QR_IMAGE_GENERATOR or Dialcodes not present")
        metrics.incCounter(config.skippedEventCount)
      }
    } catch {
      case e: Exception => println("QRCodeImageGeneratorService:CassandraUpdateFailure: " + e.getMessage)
        e.printStackTrace()
        qRCodeImageGeneratorUtil.updateCassandra(config.cassandraDialCodeBatchTable, 3, "", "processid", event.processId, metrics)
        LOGGER.info("QRCodeImageGeneratorService:CassandraUpdateFailure: " + e.getMessage)
        metrics.incCounter(config.failedEventCount)
        throw new InvalidEventException(e.getMessage, Map("partition" -> event.partition, "offset" -> event.offset), e)
    } finally {
      if (null != zipFile) {
        zipFile.delete()
      }
      availableImages.filter(imageFile => null != imageFile).foreach(imageFile => imageFile.delete())
    }
  }

  //Case class for QRCodeImageGenerator
  def getQRCodeGenerationRequest(qrImageconfig: Config, dataList: util.ArrayList[String], textList: util.ArrayList[String], fileNameList: util.ArrayList[String]): QRCodeImageGenerator = {
    val output = QRCodeImageGenerator(dataList,
      qrImageconfig.errorCorrectionLevel.get,
      qrImageconfig.pixelsPerBlock.get.asInstanceOf[Integer],
      qrImageconfig.qrCodeMargin.get.asInstanceOf[Integer],
      textList,
      qrImageconfig.textFontName.get,
      qrImageconfig.textFontSize.get,
      qrImageconfig.textCharacterSpacing.get,
      qrImageconfig.imageBorderSize.get,
      qrImageconfig.colourModel.get,
      fileNameList,
      qrImageconfig.imageFormat.get,
      qrImageconfig.qrCodeMarginBottom.getOrElse(0),
      qrImageconfig.imageMargin.getOrElse(0),
      config.lpTempFileLocation)
    println("output: " + output)
    output
  }
}
