package org.sunbird.job.functions

import java.io.{File, IOException}
import java.lang.reflect.Type
import java.util
import java.util.Base64

import com.google.gson.reflect.TypeToken
import org.apache.commons.collections.MapUtils
import org.apache.commons.io.FileUtils
import org.apache.commons.lang.StringUtils
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
import org.sunbird.incredible.processor.store.{AwsStore, AzureStore, ICertStore, StoreConfig}
import org.sunbird.incredible.processor.views.SvgGenerator
import org.sunbird.job.Exceptions.ValidationException


class CertificateGeneratorFunction(config: CertificateGeneratorConfig)
                                  (implicit val stringTypeInfo: TypeInformation[String])
  extends BaseProcessFunction[java.util.Map[String, AnyRef], String](config) {

  private[this] val logger = LoggerFactory.getLogger(classOf[CertificateGeneratorFunction])
  val mapType: Type = new TypeToken[java.util.Map[String, AnyRef]]() {}.getType
  val storageType: String = config.storageType
  val directory: String = "certificates/"
  val certStore: ICertStore = getStorageService
  lazy private val mapper: ObjectMapper = new ObjectMapper()
  mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL)

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
      generateCertificate(certReq)(metrics)
    } catch {
      case e: ValidationException =>
        metrics.incCounter(config.failedEventCount)
        logger.info("Certificate generation failed {}", e)
      //todo failed event put it into kafka topic
    }
  }

  override def metricsList(): List[String] = {
    List(config.successEventCount, config.failedEventCount, config.skippedEventCount, config.totalEventsCount)
  }


  def generateCertificate(certReq: java.util.Map[String, AnyRef])(implicit metrics: Metrics): Unit = {
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
                put(JsonKey.JSON_URL, jsonUrl)
                put(JsonKey.JSON_DATA, certificateExtension)
                put(JsonKey.ACCESS_CODE, qrMap.get(JsonKey.ACCESS_CODE))
                put(JsonKey.RECIPIENT_NAME, certModel.getRecipientName)
                put(JsonKey.RECIPIENT_ID, certModel.getIdentifier)
                put(config.RELATED, certReq.get(config.RELATED))
                val oldId: String = certReq.get(config).asInstanceOf[String]
                if (StringUtils.isNotBlank(oldId))
                  put(config.OLD_ID, oldId)
              }
            })
          }
        }
        addCertToRegistry(request)(metrics)
        //cert-registry end

      } catch {
        case e: Exception =>
          metrics.incCounter(config.failedEventCount)
          logger.info("Certificate generation failed {}", e)
        //todo failed event put it into kafka topic
      } finally {
        cleanUp(uuid, directory)
      }
    })
  }

  def addCertToRegistry(request: java.util.HashMap[String, AnyRef])(implicit metrics: Metrics): Unit = {
    val httpRequest = mapper.writeValueAsString(request)
    val httpResponse = CertificateGeneratorStreamTask.httpUtil.post(config.certRegistryBaseUrl + "/certs/v2/registry/add", httpRequest)
    if (httpResponse.status == 200) {
      logger.info("certificate added successfully to the registry " + httpResponse.body)
      metrics.incCounter(config.successEventCount)
    } else {
      metrics.incCounter(config.failedEventCount)
      logger.error("certificate addition to registry failed: " + httpResponse.status + " :: " + httpResponse.body)
      //todo failed event put it into kafka topic
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
    certStore.save(file, cloudPath)
  }

  private def populatesPropertiesMap(req: util.Map[String, AnyRef]): util.Map[String, String] = {
    val properties: util.HashMap[String, String] = new util.HashMap[String, String]
    val basePath: String = req.getOrDefault(JsonKey.BASE_PATH, config.basePath).asInstanceOf[String]
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

  @throws[Exception]
  def getStorageService: ICertStore = {
    var certStore: ICertStore = null
    val storeParams = new java.util.HashMap[String, AnyRef]
    storeParams.put(JsonKey.TYPE, storageType)
    if (StringUtils.equalsIgnoreCase(storageType, JsonKey.AZURE)) {
      val azureParams = new java.util.HashMap[String, String]
      azureParams.put(JsonKey.containerName, config.containerName)
      azureParams.put(JsonKey.ACCOUNT, config.azureStorageKey)
      azureParams.put(JsonKey.KEY, config.azureStorageSecret)
      storeParams.put(JsonKey.AZURE, azureParams)
      val storeConfig = new StoreConfig(storeParams)
      certStore = new AzureStore(storeConfig)
    } else if (StringUtils.equalsIgnoreCase(storageType, JsonKey.AWS)) {
      val awsParams = new java.util.HashMap[String, String]
      awsParams.put(JsonKey.containerName, config.containerName)
      awsParams.put(JsonKey.ACCOUNT, config.awsStorageKey)
      awsParams.put(JsonKey.KEY, config.awsStorageSecret)
      storeParams.put(JsonKey.AWS, awsParams)
      val storeConfig = new StoreConfig(storeParams)
      certStore = new AwsStore(storeConfig)

    } else throw new Exception("Error while initialising cloud storage")
    certStore
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
