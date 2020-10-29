package org.sunbird.job.functions

import java.io.{File, IOException}
import java.lang.reflect.Type
import java.text.SimpleDateFormat
import java.util
import java.util.{Base64, Date, UUID}

import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import org.apache.commons.collections.MapUtils
import org.apache.commons.io.FileUtils
import org.apache.commons.lang.StringUtils
import org.apache.commons.lang.exception.ExceptionUtils
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.task.{CertificateGeneratorConfig, CertificateGeneratorStreamTask}
import org.sunbird.job.{BaseProcessFunction, CertMapper, Metrics}
import org.sunbird.incredible.CertificateGenerator
import org.sunbird.incredible.pojos.CertificateExtension
import org.sunbird.incredible.processor.{CertModel, JsonKey}
import org.sunbird.incredible.processor.views.SvgGenerator
import org.sunbird.job.Exceptions.{ErrorCodes, ServerException, ValidationException}
import org.sunbird.job.domain.{ActorObject, Certificate, EventContext, EventData, EventObject, FailedEvent, PostCertificateProcessEvent}


class CertificateGeneratorFunction(config: CertificateGeneratorConfig)
                                  (implicit val stringTypeInfo: TypeInformation[String])
  extends BaseProcessFunction[java.util.Map[String, AnyRef], String](config) {

  private[this] val logger = LoggerFactory.getLogger(classOf[CertificateGeneratorFunction])
  val mapType: Type = new TypeToken[java.util.Map[String, AnyRef]]() {}.getType
  //  val storageType: String = config.storageType
  val directory: String = "certificates/"
  lazy private val mapper: ObjectMapper = new ObjectMapper()
  mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL)
  lazy private val gson = new Gson()
  val formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ")

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
  }

  override def close(): Unit = {
    super.close()
  }

  override def processElement(event: java.util.Map[String, AnyRef], context: ProcessFunction[java.util.Map[String, AnyRef], String]#Context, metrics: Metrics): Unit = {
    val certReq: util.Map[String, AnyRef] = event.get(config.EDATA).asInstanceOf[util.Map[String, AnyRef]]
    metrics.incCounter(config.totalEventsCount)
    try {
      new CertValidator().validateGenerateCertRequest(certReq)
      generateCertificate(certReq, context)(metrics)
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


  def pushEventToFailedTopic(context: ProcessFunction[java.util.Map[String, AnyRef], String]#Context, event: java.util.Map[String, AnyRef], errorCode: String, error: Throwable)(implicit metrics: Metrics): Unit = {
    metrics.incCounter(config.failedEventCount)
    val errorString: List[String] = ExceptionUtils.getStackTrace(error).split("\\n\\t").toList
    var stackTrace: List[String] = null
    if (errorString.length > 21) {
      stackTrace = errorString.slice(errorString.length - 21, errorString.length - 1)
    }
    event.put("failInfo", FailedEvent(errorCode, error.getMessage + " : : " + stackTrace))
    context.output(config.failedEventOutputTag, gson.toJson(event))
  }

  override def metricsList(): List[String] = {
    List(config.successEventCount, config.failedEventCount, config.skippedEventCount, config.totalEventsCount)
  }


  @throws[Exception]
  def generateCertificate(certReq: java.util.Map[String, AnyRef], context: ProcessFunction[java.util.Map[String, AnyRef], String]#Context)(implicit metrics: Metrics): Unit = {
    val properties: util.Map[String, String] = populatesPropertiesMap(certReq)
    val certModelList: java.util.List[CertModel] = new CertMapper(properties).mapReqToCertModel(certReq)
    val certificateGenerator = new CertificateGenerator(properties, directory)
    val svgGenerator = new SvgGenerator(certReq.get(JsonKey.SVG_TEMPLATE).asInstanceOf[String], directory)
    certModelList.forEach(certModel => {
      var uuid: String = null
      try {
        val certificateExtension: CertificateExtension = certificateGenerator.getCertificateExtension(certModel)
        val qrMap = certificateGenerator.generateQrCode
        val encodedQrCode: String = encodeQrCode(qrMap.get(JsonKey.QR_CODE_FILE).asInstanceOf[File])
        certificateExtension.setPrintUri(svgGenerator.generate(certificateExtension, encodedQrCode))
        uuid = certificateGenerator.getUUID(certificateExtension)
        val jsonUrl = uploadJson(certificateExtension, directory.concat(uuid).concat(".json"), certReq.get(JsonKey.TAG).asInstanceOf[String])
        //adding certificate to registry
        val request = new java.util.HashMap[String, AnyRef]() {
          {
            put(JsonKey.REQUEST, new java.util.HashMap[String, AnyRef]() {
              {
                put(JsonKey.ID, uuid)
                put(JsonKey.JSON_URL, properties.get(JsonKey.BASE_PATH).concat(jsonUrl))
                put(JsonKey.JSON_DATA, certificateExtension)
                put(JsonKey.ACCESS_CODE, qrMap.get(JsonKey.ACCESS_CODE))
                put(JsonKey.RECIPIENT_NAME, certModel.getRecipientName)
                put(JsonKey.RECIPIENT_ID, certModel.getIdentifier)
                put(config.RELATED, certReq.get(config.RELATED))
                if (certReq.containsKey(config.OLD_ID) && StringUtils.isNotBlank(certReq.get(config.OLD_ID).asInstanceOf[String]))
                  put(config.OLD_ID, certReq.get(config.OLD_ID).asInstanceOf[String])
              }
            })
          }
        }
        addCertToRegistry(certReq, request, context)(metrics)
        //cert-registry end

      } finally {
        cleanUp(uuid, directory)
      }
    })
  }

  @throws[ServerException]
  def addCertToRegistry(certReq: java.util.Map[String, AnyRef], request: java.util.HashMap[String, AnyRef], context: ProcessFunction[java.util.Map[String, AnyRef], String]#Context)(implicit metrics: Metrics): Unit = {
    val httpRequest = mapper.writeValueAsString(request)
    val httpResponse = CertificateGeneratorStreamTask.httpUtil.post(config.certRegistryBaseUrl + "/certs/v2/registry/add", httpRequest)
    if (httpResponse.status == 200) {
      logger.info("certificate added successfully to the registry " + httpResponse.body)
      metrics.incCounter(config.successEventCount)
      val certRes = request.get(JsonKey.REQUEST).asInstanceOf[java.util.Map[String, AnyRef]]
      val related = certReq.get(config.RELATED).asInstanceOf[java.util.Map[String, AnyRef]]
      val batchId = related.get(config.BATCH_ID).asInstanceOf[String]
      val courseId = related.get(config.COURSE_ID).asInstanceOf[String]
      val event = PostCertificateProcessEvent(
        ActorObject(),
        "BE_JOB_REQUEST",
        EventData(batchId,
          certRes.get(JsonKey.RECIPIENT_ID).asInstanceOf[String],
          courseId,
          certReq.get(JsonKey.COURSE_NAME).asInstanceOf[String],
          certReq.get(config.TEMPLATE_ID).asInstanceOf[String],
          Certificate(id = certRes.get(JsonKey.ID).asInstanceOf[String], name = certReq.get(JsonKey.CERTIFICATE_NAME).asInstanceOf[String], token = certRes.get(JsonKey.ACCESS_CODE).asInstanceOf[String], lastIssuedOn = formatter.format(new Date()))),
        System.currentTimeMillis(),
        EventContext(),
        s"LP.${System.currentTimeMillis()}.${UUID.randomUUID().toString}",
        EventObject(batchId.concat("_".concat(courseId)))
      )
      context.output(config.postCertificateProcessEventOutputTag, gson.toJson(event))
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
    CertificateGeneratorStreamTask.getStorageService(config).save(file, cloudPath.concat("/"))
  }

  private def populatesPropertiesMap(req: util.Map[String, AnyRef]): util.Map[String, String] = {
    val properties: util.HashMap[String, String] = new util.HashMap[String, String]
    val basePath: String = req.getOrDefault(req.get(JsonKey.BASE_PATH), config.basePath).asInstanceOf[String]
    val tag: String = req.get(JsonKey.TAG).asInstanceOf[String]
    val keysObject: util.Map[String, AnyRef] = req.get(JsonKey.KEYS).asInstanceOf[util.Map[String, AnyRef]]
    if (MapUtils.isNotEmpty(keysObject)) {
      val keyId: String = keysObject.get(JsonKey.ID).asInstanceOf[String]
      properties.put(JsonKey.KEY_ID, keyId)
      properties.put(JsonKey.SIGN_CREATOR, basePath.concat("/").concat(keyId).concat(config.PUBLIC_KEY_URL))
      properties.put(JsonKey.PUBLIC_KEY_URL, basePath.concat("/").concat(JsonKey.KEYS).concat("/").concat(keyId).concat(config.PUBLIC_KEY_URL))
      logger.info("populatePropertiesMap: keys after {}", keyId)
    }
    properties.put(JsonKey.BASE_PATH, basePath)
    properties.put(JsonKey.TAG, tag)
    properties.put(JsonKey.BADGE_URL, basePath.concat(if (StringUtils.isNotBlank(tag)) "/".concat(tag).concat(config.BADGE_URL) else config.BADGE_URL))
    properties.put(JsonKey.ISSUER_URL, basePath.concat(config.ISSUER_URL))
    properties.put(JsonKey.EVIDENCE_URL, basePath.concat(config.EVIDENCE_URL))
    properties.put(JsonKey.CONTEXT, basePath.concat(config.CONTEXT))
    properties.put(JsonKey.ACCESS_CODE_LENGTH, config.ACCESS_CODE_LENGTH)
    properties.put(JsonKey.ENC_SERVICE_URL, config.encServiceUrl)
    properties.put(JsonKey.SIGNATORY_EXTENSION, String.format("%s/%s/%s", basePath, config.SIGNATORY_EXTENSION, "context.json"))
    properties
  }

  private def cleanUp(fileName: String, path: String): Unit = {
    try {
      val directory = new File(path)
      val files: Array[File] = directory.listFiles
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
