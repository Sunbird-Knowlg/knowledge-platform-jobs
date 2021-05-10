package org.sunbird.job.functions

import java.io.File
import java.util

import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.qrcodeimagegenerator.domain.{Config, QRCodeGenerationRequest}
import org.sunbird.job.task.QRCodeImageGeneratorConfig
import org.sunbird.job.util.{CassandraUtil, CloudStorageUtil, JSONUtil, QRCodeImageGeneratorUtil, ZipEditorUtil}
import org.sunbird.job.{BaseProcessFunction, Metrics}

class QRCodeImageGeneratorFunction(config: QRCodeImageGeneratorConfig)
                                  (implicit mapTypeInfo: TypeInformation[util.Map[String, AnyRef]])
                          extends BaseProcessFunction[util.Map[String,AnyRef], String](config) {

    private val LOGGER = LoggerFactory.getLogger(classOf[QRCodeImageGeneratorFunction])
    var cloudStorageUtil: CloudStorageUtil = _
    val qRCodeImageGeneratorUtil = new QRCodeImageGeneratorUtil(config)
    var cassandraUtil: CassandraUtil = _

    override def metricsList(): List[String] = {
        List(config.totalEventsCount, config.successEventCount, config.failedEventCount, config.skippedEventCount)
    }

    override def open(parameters: Configuration): Unit = {
        cloudStorageUtil = new CloudStorageUtil(config)
        cassandraUtil = new CassandraUtil(config.cassandraHost, config.cassandraPort)
        super.open(parameters)
    }

    override def close(): Unit = {
        cassandraUtil.close()
        super.close()
    }

    override def processElement(event: util.Map[String, AnyRef],
                                context: ProcessFunction[util.Map[String, AnyRef], String]#Context,
                                metrics: Metrics): Unit = {
        metrics.incCounter(config.totalEventsCount)
        val qrCodeImageGeneratorReq = JSONUtil.deserialize[QRCodeGenerationRequest](JSONUtil.serialize(event))
        println("event: " + event)
        println("map: " + qrCodeImageGeneratorReq)
        val availableImages = new util.ArrayList[File]

        var zipFile: File = null
        try {
            LOGGER.info("QRCodeImageGeneratorService:processMessage: Processing request: " + event);
            LOGGER.info("QRCodeImageGeneratorService:processMessage: Starting message processing at " + System.currentTimeMillis());

            if (!event.containsKey(qrCodeImageGeneratorReq.eid) && qrCodeImageGeneratorReq.eid.equalsIgnoreCase(config.eid)) {
                val eid = qrCodeImageGeneratorReq.eid
                val qrImageconfig = qrCodeImageGeneratorReq.config
                println("config: " + config)
                println("eid: " + eid)

                val dialCodes = event.getOrDefault("dialcodes", List[Map[String, AnyRef]]()).asInstanceOf[List[Map[String, AnyRef]]]
                println("dialcodes: " + dialCodes)
                val imageFormat: String = qrImageconfig.imageFormat.getOrElse("png")

                val dataList = new util.ArrayList[String]
                val textList = new util.ArrayList[String]
                val fileNameList = new util.ArrayList[String]
                var downloadUrl: String = null

                val tempFilePath = config.lpTempfileLocation

                import scala.collection.JavaConversions._
                for (dialCode <- dialCodes) {
                    println("location present or not: " + dialCode.containsKey("location"))
                    if (dialCode.containsKey("location")) {
                        try {
                            downloadUrl = dialCode.get("location").asInstanceOf[String]
                            val fileName = dialCode.get("id").asInstanceOf[String]
                            val fileToSave = new File(tempFilePath + File.separator + fileName + "." + imageFormat)
                            LOGGER.info("QRCodeImageGeneratorService:processMessage: creating file - " + fileToSave.getAbsolutePath)
                            fileToSave.createNewFile
                            LOGGER.info("QRCodeImageGeneratorService:processMessage: created file - " + fileToSave.getAbsolutePath)
                            //                            cloudStorageUtil.downloadFile(downloadUrl, fileToSave)
                            availableImages.add(fileToSave)
                        } catch {
                            case e: Exception =>
                                LOGGER.error("QRCodeImageGeneratorService:processMessage: Error while downloading image: " + downloadUrl + " With exception: " + e)
                        }
                    }
                    dataList.add(dialCode.getOrElse("data", "").asInstanceOf[String])
                    textList.add(dialCode.getOrElse("text", "").asInstanceOf[String])
                    fileNameList.add(dialCode.getOrElse("id", "").asInstanceOf[String])

                    println("dataList: " + dataList)
                    println("dataList: " + textList)
                    println("dataList: " + fileNameList)
                }
                val storage = qrCodeImageGeneratorReq.storage

                val qrGenRequest: org.sunbird.job.model.QRCodeGenerationRequest = getQRCodeGenerationRequest(qrImageconfig, dataList, textList, fileNameList)
                println("qrGenReqyest: " + JSONUtil.serialize(qrGenRequest))

                val generatedImages = qRCodeImageGeneratorUtil.createQRImages(qrGenRequest, config, storage.container, storage.path)
                println("generatedImages: " + generatedImages)
                if (!StringUtils.isBlank(qrCodeImageGeneratorReq.processId)) {
                    val processId = qrCodeImageGeneratorReq.processId
                    var zipFileName = storage.fileName
                    LOGGER.info("QRCodeImageGeneratorService:processMessage: Generating zip for QR codes with processId " + qrCodeImageGeneratorReq.processId)
                    if (StringUtils.isBlank(zipFileName)) zipFileName = processId
                    availableImages.addAll(generatedImages)
                    zipFile = ZipEditorUtil.zipFiles(availableImages, zipFileName, tempFilePath)
                    val zipDownloadUrl = cloudStorageUtil.uploadFile(storage.container, storage.path, zipFile, false)
                    cassandraUtil.updateDownloadZIPUrl(processId, zipDownloadUrl)
                }
                else {
                    LOGGER.info("QRCodeImageGeneratorService:processMessage: Skipping zip creation due to missing processId.")
                }
                LOGGER.info("QRCodeImageGeneratorService:processMessage: Message processed successfully at " + System.currentTimeMillis)
            } else {
                LOGGER.info("QRCodeImageGeneratorService: Eid other than BE_QR_IMAGE_GENERATOR")
                metrics.incCounter(config.skippedEventCount)
            }
        } catch {
            case e: Exception => cassandraUtil.updateFailure(event.get("processId").asInstanceOf[String], e.getMessage)
            LOGGER.info("QRCodeImageGeneratorService:CassandraUpdateFailure: " + e.getMessage)
            throw e
        } finally {
            if (null != zipFile)
                {
                    zipFile.delete()
                }
            import scala.collection.JavaConversions._
            for (imageFile <- availableImages) {
                if (null != imageFile) {
                    imageFile.delete()
                }
            }
        }
    }

    private def getQRCodeGenerationRequest(qrImageconfig: Config, dataList: util.List[String], textList: util.List[String], fileNameList: util.List[String]) = {
        import org.sunbird.job.model.QRCodeGenerationRequest
        val qrGenRequest = new QRCodeGenerationRequest
        qrGenRequest.setData(dataList)
        qrGenRequest.setText(textList)
        qrGenRequest.setFileName(fileNameList)
        qrGenRequest.setErrorCorrectionLevel(qrImageconfig.errorCorrectionLevel.get.asInstanceOf[String])
        qrGenRequest.setPixelsPerBlock(qrImageconfig.pixelsPerBlock.get.asInstanceOf[Integer])
        qrGenRequest.setQrCodeMargin(qrImageconfig.qrCodeMargin.get.asInstanceOf[Integer])
        qrGenRequest.setTextFontName(qrImageconfig.textFontName.get.asInstanceOf[String])
        qrGenRequest.setTextFontSize(qrImageconfig.textFontSize.get.asInstanceOf[Integer])
        qrGenRequest.setTextCharacterSpacing(qrImageconfig.textCharacterSpacing.get.asInstanceOf[Double])
        qrGenRequest.setFileFormat(qrImageconfig.imageFormat.get.asInstanceOf[String])
        qrGenRequest.setColorModel(qrImageconfig.colourModel.get)
        qrGenRequest.setImageBorderSize(qrImageconfig.imageBorderSize.get.asInstanceOf[Integer])
        qrGenRequest.setQrCodeMarginBottom(qrImageconfig.qrCodeMarginBottom.getOrElse(config.qrImageBottomMargin))
        qrGenRequest.setImageMargin(qrImageconfig.imageMargin.getOrElse(config.qrImageMargin))
        qrGenRequest.setTempFilePath(config.lpTempfileLocation)
        qrGenRequest
    }
}
