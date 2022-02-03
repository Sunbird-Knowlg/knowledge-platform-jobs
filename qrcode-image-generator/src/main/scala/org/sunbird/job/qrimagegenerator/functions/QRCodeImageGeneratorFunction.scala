package org.sunbird.job.qrimagegenerator.functions

import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.exception.InvalidEventException
import org.sunbird.job.qrimagegenerator.domain.{Config, Event, QRCodeImageGenerator}
import org.sunbird.job.qrimagegenerator.task.QRCodeImageGeneratorConfig
import org.sunbird.job.qrimagegenerator.util.QRCodeImageGeneratorUtil
import org.sunbird.job.util.{CassandraUtil, CloudStorageUtil, FileUtils}
import org.sunbird.job.{BaseProcessFunction, Metrics}

import java.io.File
import scala.collection.mutable.ListBuffer

class QRCodeImageGeneratorFunction(config: QRCodeImageGeneratorConfig,
                                   @transient var cassandraUtil: CassandraUtil = null,
                                   @transient var cloudStorageUtil: CloudStorageUtil = null,
                                   @transient var qRCodeImageGeneratorUtil: QRCodeImageGeneratorUtil = null)
                                  (implicit val stringTypeInfo: TypeInformation[String])
  extends BaseProcessFunction[Event, String](config) {

  private val logger = LoggerFactory.getLogger(classOf[QRCodeImageGeneratorFunction])

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

  override def metricsList(): List[String] = {
    List(config.totalEventsCount, config.successEventCount, config.failedEventCount, config.skippedEventCount, config.dbFailureEventCount,
      config.dbHitEventCount, config.cloudDbHitCount, config.cloudDbFailCount)
  }

  @throws(classOf[InvalidEventException])
  override def processElement(event: Event,
                              context: ProcessFunction[Event, String]#Context,
                              metrics: Metrics): Unit = {
    metrics.incCounter(config.totalEventsCount)

    val availableImages = ListBuffer[File]()
    var zipFile: File = null
    try {
      logger.info("QRCodeImageGeneratorService:processMessage: Processing request for processId : " + event.processId + " and objectId: " + event.objectId)
      logger.info("QRCodeImageGeneratorService:processMessage: Starting message processing at " + System.currentTimeMillis())
      if (event.isValid(config)) {
        val tempFilePath = config.lpTempFileLocation

        event.dialCodes.filter(f => !StringUtils.equals(f.getOrElse("location", "").asInstanceOf[String], ""))
          .foreach { dialcode =>
            try {
              val fileName = dialcode.getOrElse("id", "")
              val destPath = s"""$tempFilePath${File.separator}$fileName.${event.imageFormat}"""
              val downloadUrl = dialcode("location").asInstanceOf[String]
              val file: File = FileUtils.downloadFile(downloadUrl, destPath)
              logger.info("QRCodeImageGeneratorService:processMessage: created file - " + file.getAbsolutePath)
              metrics.incCounter(config.cloudDbHitCount)
              availableImages += file
            } catch {
              case e: Exception =>
                metrics.incCounter(config.cloudDbFailCount)
                throw new InvalidEventException(e.getMessage, Map("partition" -> event.partition, "offset" -> event.offset), e)
            }
          }
        logger.info("availableImages after W/0 Loc: " + availableImages)

        val dataList = ListBuffer[String]()
        val textList = ListBuffer[String]()
        val fileNameList = ListBuffer[String]()
        event.dialCodes.map { dialcode =>
          dataList += dialcode("data").asInstanceOf[String]
          textList += dialcode("text").asInstanceOf[String]
          fileNameList += dialcode("id").asInstanceOf[String]
        }

        val qrGenRequest: QRCodeImageGenerator = getQRCodeGenerationRequest(event.imageConfig, dataList, textList, fileNameList)
        logger.info("qrGenRequest: " + qrGenRequest)
        val generatedImages: ListBuffer[File] = qRCodeImageGeneratorUtil.createQRImages(qrGenRequest, event.storageContainer, event.storagePath, metrics)

        if (!StringUtils.isBlank(event.processId)) {
          var storageFileName = event.storageFileName
          logger.info("QRCodeImageGeneratorService:processMessage: Generating zip for QR codes with processId " + event.processId)
          if (StringUtils.isBlank(storageFileName)) storageFileName = event.processId

          // Merge available and generated image list
          generatedImages.foreach(f => availableImages += f)

//          zipFile = ZipEditorUtil.zipFiles(availableImages.toList, storageFileName, tempFilePath)
          val zipFileName: String = tempFilePath + File.separator + storageFileName + ".zip"
          val fileList: List[String] = availableImages.map(f => f.getName).toList
          FileUtils.zipIt(zipFileName, fileList, tempFilePath)

          zipFile = new File(zipFileName)

          val zipDownloadUrl = cloudStorageUtil.uploadFile(event.storagePath, zipFile, Some(false), container = event.storageContainer)
          metrics.incCounter(config.cloudDbHitCount)
          qRCodeImageGeneratorUtil.updateCassandra(config.cassandraDialCodeBatchTable, 2, zipDownloadUrl(1), "processid", event.processId, metrics)
        }
        else {
          logger.info("QRCodeImageGeneratorService:processMessage: Skipping zip creation due to missing processId.")
        }
        logger.info("QRCodeImageGeneratorService:processMessage: Message processed successfully at " + System.currentTimeMillis)
      } else {
        logger.info("QRCodeImageGeneratorService: Eid other than BE_QR_IMAGE_GENERATOR or Dialcodes not present")
        metrics.incCounter(config.skippedEventCount)
      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
        qRCodeImageGeneratorUtil.updateCassandra(config.cassandraDialCodeBatchTable, 3, "", "processid", event.processId, metrics)
        logger.info("QRCodeImageGeneratorService:CassandraUpdateFailure: " + e.getMessage)
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
  def getQRCodeGenerationRequest(qrImageconfig: Config, dataList: ListBuffer[String], textList: ListBuffer[String], fileNameList: ListBuffer[String]): QRCodeImageGenerator = {
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
    logger.info("output: " + output)
    output
  }
}
