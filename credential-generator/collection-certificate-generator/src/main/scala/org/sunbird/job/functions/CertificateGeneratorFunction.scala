package org.sunbird.job.functions

import java.io.{File, IOException}
import java.lang.reflect.Type
import java.text.SimpleDateFormat
import java.util
import java.util.stream.Collectors
import java.util.{Base64, Date}

import com.datastax.driver.core.{Row, TypeTokens}
import com.datastax.driver.core.querybuilder.{QueryBuilder, Update}
import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import org.apache.commons.io.FileUtils
import org.apache.commons.lang.StringUtils
import org.apache.commons.lang.exception.ExceptionUtils
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.incredible.pojos.ob.CertificateExtension
import org.sunbird.incredible.{CertificateConfig, CertificateGenerator, JsonKeys, JsonUtils}
import org.sunbird.incredible.processor.CertModel
import org.sunbird.incredible.processor.store.StorageService
import org.sunbird.incredible.processor.views.SvgGenerator
import org.sunbird.job.Exceptions.{ErrorCodes, ServerException, ValidationException}
import org.sunbird.job.domain.{Actor, Certificate, CertificateAuditEvent, Event, EventContext, EventData, EventObject, FailedEvent, UserEnrollmentData}
import org.sunbird.job.task.CertificateGeneratorConfig
import org.sunbird.job.{BaseProcessFunction, Metrics}
import org.sunbird.job.util.{CassandraUtil, HttpUtil}
import org.sunbird.notifier.NotificationMetaData
import org.sunbird.user.feeds.UserFeedMetaData

import scala.collection.JavaConverters._

class CertificateGeneratorFunction(config: CertificateGeneratorConfig, httpUtil: HttpUtil, storageService: StorageService, @transient var cassandraUtil: CassandraUtil = null)
  extends BaseProcessFunction[Event, String](config) {


  private[this] val logger = LoggerFactory.getLogger(classOf[CertificateGeneratorFunction])
  val mapType: Type = new TypeToken[java.util.Map[String, AnyRef]]() {}.getType
  val directory: String = "certificates/"
  lazy private val gson = new Gson()
  val formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ")
  implicit val certificateConfig: CertificateConfig = CertificateConfig(basePath = config.basePath, encryptionServiceUrl = config.encServiceUrl, contextUrl = config.CONTEXT, issuerUrl = config.ISSUER_URL,
    evidenceUrl = config.EVIDENCE_URL, signatoryExtension = config.SIGNATORY_EXTENSION)

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
    val eData = event.eData + "failInfo" -> FailedEvent(errorCode, error.getMessage + " : : " + stackTrace)
    context.output(config.failedEventOutputTag, gson.toJson(eData))
  }

  override def metricsList(): List[String] = {
    List(config.successEventCount, config.failedEventCount, config.skippedEventCount, config.totalEventsCount, config.dbUpdateCount, config.enrollmentDbReadCount, config.totalEventsCount, config.skipNotifyUserCount, config.notifiedUserCount)
  }


  @throws[Exception]
  def generateCertificate(event: Event, context: ProcessFunction[Event, String]#Context)(implicit metrics: Metrics): Unit = {
    val certModelList: List[CertModel] = new CertMapper(certificateConfig).mapReqToCertModel(event)
    println("certificatelis " + certModelList)
    val certificateGenerator = new CertificateGenerator
    certModelList.foreach(certModel => {
      var uuid: String = null
      try {
        val certificateExtension: CertificateExtension = certificateGenerator.getCertificateExtension(certModel)
        uuid = certificateGenerator.getUUID(certificateExtension)
        val qrMap = certificateGenerator.generateQrCode(uuid, directory, certificateConfig.basePath)
        val encodedQrCode: String = encodeQrCode(qrMap.qrFile)
        val printUri = SvgGenerator.generate(certificateExtension, encodedQrCode, event.svgTemplate)
        certificateExtension.printUri = Option(printUri)
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

        val related = event.related
        val userEnrollmentData = UserEnrollmentData(related.getOrElse(config.BATCH_ID, "").asInstanceOf[String], certModel.identifier,
          related.getOrElse(config.COURSE_ID, "").asInstanceOf[String], event.courseName, event.templateId,
          Certificate(uuid, event.name, qrMap.accessCode, formatter.format(new Date())))
        updateUserEnrollmentTable(event, userEnrollmentData, context)
        metrics.incCounter(config.successEventCount)
      } finally {
        cleanUp(uuid, directory)
      }
    })
  }

  @throws[ServerException]
  def addCertToRegistry(certReq: Event, request: java.util.HashMap[String, AnyRef], context: ProcessFunction[Event, String]#Context)(implicit metrics: Metrics): Unit = {
    logger.info("adding certificate to the registry")
    val httpRequest = JsonUtils.serialize(request)
    val httpResponse = httpUtil.post(config.certRegistryBaseUrl + "/certs/v2/registry/add", httpRequest)
    if (httpResponse.status == 200) {
      logger.info("certificate added successfully to the registry " + httpResponse.body)
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
    JsonUtils.writeToJsonFile(file, certificateExtension)
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


  def updateUserEnrollmentTable(event: Event, certMetaData: UserEnrollmentData, context: ProcessFunction[Event, String]#Context)(implicit metrics: Metrics): Unit = {
    logger.info("updating user enrollment table {}", certMetaData)
    val primaryFields = Map(config.userId.toLowerCase() -> certMetaData.userId, config.batchId.toLowerCase -> certMetaData.batchId, config.courseId.toLowerCase -> certMetaData.courseId)
    val records = getIssuedCertificatesFromUserEnrollmentTable(primaryFields)
    if (records.nonEmpty) {
      records.foreach((row: Row) => {
        val issuedOn = row.getTimestamp("completedOn")
        var certificatesList = row.getList(config.issued_certificates, TypeTokens.mapOf(classOf[String], classOf[String]))
        if (null == certificatesList && certificatesList.isEmpty) {
          certificatesList = new util.ArrayList[util.Map[String, String]]()
        }
        val updatedCerts: util.List[util.Map[String, String]] = certificatesList.stream().filter(cert => !StringUtils.equalsIgnoreCase(certMetaData.certificate.name, cert.get("name"))).collect(Collectors.toList())
        updatedCerts.add(new java.util.HashMap[String, String]() {
          {
            put(config.name, certMetaData.certificate.name)
            put(config.identifier, certMetaData.certificate.id)
            put(config.token, certMetaData.certificate.token)
            if (StringUtils.isNotBlank(certMetaData.certificate.lastIssuedOn))
              put(config.lastIssuedOn, certMetaData.certificate.lastIssuedOn)
          }
        })
        val query = getUpdateIssuedCertQuery(updatedCerts, certMetaData.userId, certMetaData.courseId, certMetaData.batchId, config)
        logger.info("update query {}", query.toString)
        val result = cassandraUtil.update(query)
        logger.info("update result {}", result)
        if (result) {
          logger.info("issued certificates in user-enrollment table  updated successfully")
          metrics.incCounter(config.dbUpdateCount)
          val certificateAuditEvent = generateAuditEvent(certMetaData)
          logger.info("pushAuditEvent: audit event generated for certificate : " + certificateAuditEvent)
          context.output(config.auditEventOutputTag, gson.toJson(certificateAuditEvent))
          logger.info("pushAuditEvent: certificate audit event success")
          context.output(config.notifierOutputTag, NotificationMetaData(certMetaData.userId, certMetaData.courseName, issuedOn, certMetaData.courseId, certMetaData.batchId, certMetaData.templateId))
          context.output(config.userFeedOutputTag, UserFeedMetaData(certMetaData.userId, certMetaData.courseName, issuedOn))
        } else {
          metrics.incCounter(config.failedEventCount)
          val edata = event.eData + "failInfo" -> FailedEvent("ERR_DB_UPDATION_FAILED", "db update failed")
          context.output(config.failedEventOutputTag, gson.toJson(edata))
          logger.info("Database update has failed {}", query)
        }

      })
    }

  }


  /**
    * returns query for updating issued_certificates in user_enrollment table
    */
  def getUpdateIssuedCertQuery(updatedCerts: util.List[util.Map[String, String]], userId: String, courseId: String, batchId: String, config: CertificateGeneratorConfig):
  Update.Where = QueryBuilder.update(config.dbKeyspace, config.dbEnrollmentTable).where()
    .`with`(QueryBuilder.set(config.issued_certificates, updatedCerts))
    .where(QueryBuilder.eq(config.userId.toLowerCase, userId))
    .and(QueryBuilder.eq(config.courseId.toLowerCase, courseId))
    .and(QueryBuilder.eq(config.batchId.toLowerCase, batchId))


  private def getIssuedCertificatesFromUserEnrollmentTable(columns: Map[String, AnyRef])(implicit metrics: Metrics) = {
    logger.info("primary columns {}", columns)
    val selectWhere = QueryBuilder.select().all()
      .from(config.dbKeyspace, config.dbEnrollmentTable).
      where()
    columns.map(col => {
      col._2 match {
        case value: List[Any] =>
          selectWhere.and(QueryBuilder.in(col._1, value.asJava))
        case _ =>
          selectWhere.and(QueryBuilder.eq(col._1, col._2))
      }
    })
    logger.info("select query {}", selectWhere.toString)
    metrics.incCounter(config.enrollmentDbReadCount)
    cassandraUtil.find(selectWhere.toString).asScala.toList
  }


  private def generateAuditEvent(data: UserEnrollmentData): CertificateAuditEvent = {
    CertificateAuditEvent(
      actor = Actor(id = data.userId),
      edata = EventData(props = Array("certificates"), `type` = "certificate-issued-svg"),
      context = EventContext(cdata = Array(Map("type" -> config.courseBatch, config.id -> data.batchId).asJava)),
      `object` = EventObject(id = data.certificate.id, `type` = "Certificate", rollup = Map(config.l1 -> data.courseId).asJava))
  }


}
