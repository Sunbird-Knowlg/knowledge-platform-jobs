package org.sunbird.job.functions

import java.io.{File, IOException}
import java.lang.reflect.Type
import java.text.SimpleDateFormat
import java.util.{Base64, Date}

import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import org.apache.commons.io.FileUtils
import org.apache.commons.lang.StringUtils
import org.apache.commons.lang.exception.ExceptionUtils
import org.apache.flink.configuration.Configuration
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.incredible.pojos.ob.CertificateExtension
import org.sunbird.incredible.{CertificateConfig, CertificateGenerator, CertificateProperties, JsonKeys, QrCodeModel, StorageParams}
import org.sunbird.incredible.processor.CertModel
import org.sunbird.incredible.processor.store.StorageService
import org.sunbird.incredible.processor.views.SvgGenerator
import org.sunbird.job.Exceptions.{ErrorCodes, ServerException, ValidationException}
import org.sunbird.job.domain.{Certificate, Event, EventContext, EventData, EventObject, FailedEvent, PostCertificateProcessEvent}
import org.sunbird.job.task.CertificateGeneratorConfig
import org.sunbird.job.{BaseProcessFunction, Metrics}
import org.sunbird.job.util.{CassandraUtil, HttpUtil}
import scala.collection.JavaConverters._

class CertificateGeneratorFunction(config: CertificateGeneratorConfig, httpUtil: HttpUtil, @transient var cassandraUtil: CassandraUtil = null)
  extends BaseProcessFunction[Event, String](config) {


  private[this] val logger = LoggerFactory.getLogger(classOf[CertificateGeneratorFunction])
  val mapType: Type = new TypeToken[java.util.Map[String, AnyRef]]() {}.getType
  val directory: String = "certificates/"
  lazy private val mapper: ObjectMapper = new ObjectMapper()
  mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL)
  lazy private val gson = new Gson()
  val formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ")
  implicit val certificateConfig: CertificateConfig = CertificateConfig(basePath = config.basePath, encryptionServiceUrl = config.encServiceUrl, contextUrl = config.CONTEXT, issuerUrl = config.ISSUER_URL,
    evidenceUrl = config.EVIDENCE_URL, signatoryExtension = config.SIGNATORY_EXTENSION)
  val storageParams: StorageParams = StorageParams(config.storageType, config.azureStorageKey, config.azureStorageSecret, config.containerName)
  val storageService: StorageService = new StorageService(storageParams)
  val postCertificateProcessor: PostCertificateProcessor = new PostCertificateProcessor(config, httpUtil)(cassandraUtil)

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    cassandraUtil = new CassandraUtil(config.dbHost, config.dbPort)
  }

  override def close(): Unit = {
    cassandraUtil.close()
    super.close()
  }

  override def processElement(event: Event,
                              context: ProcessFunction[Event, String]#Context,
                              metrics: Metrics): Unit = {
    println("Certificate data: " + event)
    metrics.incCounter(config.totalEventsCount)
    try {
      new CertValidator().validateGenerateCertRequest(event)
      generateCertificate(event, context)(metrics)
    } catch {
      case e: ValidationException =>
        logger.info("Certificate generation failed {}", e)
        pushEventToFailedTopic(context, event, e.errorCode, e)(metrics)
      case e: ServerException =>
        logger.info("Certificate generation failed {}", e)
        pushEventToFailedTopic(context, event, ErrorCodes.SYSTEM_ERROR, e)(metrics)
      case e: Exception =>
        logger.info("Certificate generation failed {}", e)
        pushEventToFailedTopic(context, event, ErrorCodes.SYSTEM_ERROR, e)(metrics)
    }
  }

  def pushEventToFailedTopic(context: ProcessFunction[Event, String]#Context, event: Event, errorCode: String, error: Throwable)(implicit metrics: Metrics): Unit = {
    metrics.incCounter(config.failedEventCount)
    val errorString: List[String] = ExceptionUtils.getStackTrace(error).split("\\n\\t").toList
    var stackTrace: List[String] = null
    if (errorString.length > 21) {
      stackTrace = errorString.slice(errorString.length - 21, errorString.length - 1)
    }
    //    event.put("failInfo", FailedEvent(errorCode, error.getMessage + " : : " + stackTrace))
    context.output(config.failedEventOutputTag, gson.toJson(event))
  }

  override def metricsList(): List[String] = {
    List(config.successEventCount, config.failedEventCount, config.skippedEventCount, config.totalEventsCount)
  }


  @throws[Exception]
  def generateCertificate(event: Event, context: ProcessFunction[Event, String]#Context)(implicit metrics: Metrics): Unit = {
    val properties: CertificateProperties = CertificateProperties(event.tag, event.keys.getOrElse(JsonKeys.ID, ""))
    val certModelList: List[CertModel] = new CertMapper(certificateConfig).mapReqToCertModel(event)
    val certificateGenerator = new CertificateGenerator
    certModelList.foreach(certModel => {
      var uuid: String = null
      try {
        val certificateExtension: CertificateExtension = certificateGenerator.getCertificateExtension(properties, certModel)
        uuid = certificateGenerator.getUUID(certificateExtension)
        val qrMap = certificateGenerator.generateQrCode(uuid, directory, certificateConfig.basePath)
        val encodedQrCode: String = encodeQrCode(qrMap.qrFile)
        certificateExtension.printUri = Option.apply(SvgGenerator.generate(certificateExtension, encodedQrCode, event.svgTemplate))
        val jsonUrl = uploadJson(certificateExtension, directory.concat(uuid).concat(".json"), event.tag)
        //adding certificate to registry
        val addReq = new java.util.HashMap[String, AnyRef]() {
          {
            put(JsonKeys.REQUEST, new java.util.HashMap[String, AnyRef]() {
              {
                put(JsonKeys.ID, uuid)
                put(JsonKeys.JSON_URL, certificateConfig.basePath.concat(jsonUrl))
                put(JsonKeys.JSON_DATA, certificateExtension)
                put(JsonKeys.ACCESS_CODE, qrMap.accessCode)
                put(JsonKeys.RECIPIENT_NAME, certModel.recipientName)
                put(JsonKeys.RECIPIENT_ID, certModel.identifier)
                put(config.RELATED, event.related)
                val oldId = event.oldId
                if (StringUtils.isNotBlank(oldId))
                  put(config.OLD_ID, oldId)
              }
            })
          }
        }
        addCertToRegistry(event, addReq, context)(metrics)
        //cert-registry end

      } finally {
        cleanUp(uuid, directory)
      }
    })
  }

  @throws[ServerException]
  def addCertToRegistry(certReq: Event, request: java.util.HashMap[String, AnyRef], context: ProcessFunction[Event, String]#Context)(implicit metrics: Metrics): Unit = {
    val httpRequest = mapper.writeValueAsString(request)
    val httpResponse = httpUtil.post(config.certRegistryBaseUrl + "/certs/v2/registry/add", httpRequest)
    if (httpResponse.status == 200) {
      logger.info("certificate added successfully to the registry " + httpResponse.body)
      metrics.incCounter(config.successEventCount)

      val certRes = request.get(JsonKeys.REQUEST).asInstanceOf[java.util.Map[String, AnyRef]]
      val related = certReq.related
      val batchId = related.get(config.BATCH_ID).asInstanceOf[String]
      val courseId = related.get(config.COURSE_ID).asInstanceOf[String]

      val event = EventData(batchId,
        certRes.get(JsonKeys.RECIPIENT_ID).asInstanceOf[String],
        courseId,
        certReq.courseName,
        certReq.templateId,
        Certificate(certRes.get(JsonKeys.ID).asInstanceOf[String], certReq.name, certRes.get(JsonKeys.ACCESS_CODE).asInstanceOf[String], formatter.format(new Date())),
        "post-process-certificate", 1)
      postCertificateProcessor.process(event, context)(metrics)
    } else {
      logger.error("certificate addition to registry failed: " + httpResponse.status + " :: " + httpResponse.body)
      throw ServerException("ERR_API_CALL", "Something Went Wrong While Making API Call | Status is: " + httpResponse.status + " :: " + httpResponse.body)
    }
  }

  @throws[IOException]
  private def encodeQrCode(file: File): String = {
    val fileContent = FileUtils.readFileToByteArray(file)
    file.delete
    Base64.getEncoder.encodeToString(fileContent)
  }

  @throws[IOException]
  private def uploadJson(certificateExtension: CertificateExtension, fileName: String, cloudPath: String): String = {
    logger.info("uploadJson: uploading json file started {}", fileName)
    val file = new File(fileName)
    mapper.writeValue(file, certificateExtension)
    storageService.uploadFile(cloudPath, file)
  }

  private def cleanUp(fileName: String, path: String): Unit = {
    try {
      val directory = new File(path)
      val files: Array[File] = directory.listFiles
      if (files != null && files.length > 0)
        files.foreach(file => {
          if (file.getName.startsWith(fileName)) file.delete
        })
      logger.info("cleanUp completed")
    } catch {
      case ex: Exception =>
        logger.error(ex.getMessage, ex)
    }
  }

}
