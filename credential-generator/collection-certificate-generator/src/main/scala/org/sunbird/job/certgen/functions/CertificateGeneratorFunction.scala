package org.sunbird.job.certgen.functions

import com.datastax.driver.core.querybuilder.{QueryBuilder, Update}
import com.datastax.driver.core.{Row, TypeTokens}
import com.google.gson.reflect.TypeToken
import org.apache.commons.lang.StringUtils
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.incredible.processor.CertModel
import org.sunbird.incredible.processor.store.StorageService
import org.sunbird.incredible.{CertificateConfig, JsonKeys, ScalaModuleJsonUtils}
import org.sunbird.job.certgen.domain._
import org.sunbird.job.certgen.exceptions.ServerException
import org.sunbird.job.certgen.task.CertificateGeneratorConfig
import org.sunbird.job.exception.InvalidEventException
import org.sunbird.job.util.{CassandraUtil, HttpUtil, ScalaJsonUtil}
import org.sunbird.job.{BaseProcessKeyedFunction, Metrics}

import java.io.File
import java.lang.reflect.Type
import java.text.SimpleDateFormat
import java.util
import java.util.stream.Collectors
import java.util.Date
import scala.collection.JavaConverters._

class CertificateGeneratorFunction(config: CertificateGeneratorConfig, httpUtil: HttpUtil, storageService: StorageService, @transient var cassandraUtil: CassandraUtil = null)
  extends BaseProcessKeyedFunction[String, Event, String](config) {


  private[this] val logger = LoggerFactory.getLogger(classOf[CertificateGeneratorFunction])
  val mapType: Type = new TypeToken[java.util.Map[String, AnyRef]]() {}.getType
  val directory: String = "certificates/"
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

  override def metricsList(): List[String] = {
    List(config.successEventCount, config.failedEventCount, config.skippedEventCount, config.totalEventsCount, config.dbUpdateCount, config.enrollmentDbReadCount, config.totalEventsCount)
  }


  override def processElement(event: Event,
                              context: KeyedProcessFunction[String, Event, String]#Context,
                              metrics: Metrics): Unit = {
    println("Certificate data: " + event)
    metrics.incCounter(config.totalEventsCount)
    try {
      val certValidator = new CertValidator()
      certValidator.validateGenerateCertRequest(event, config.enableSuppressException)
      if(certValidator.isNotIssued(event)(config, metrics, cassandraUtil))
        generateCertificate(event, context)(metrics)
      else {
        metrics.incCounter(config.skippedEventCount)
        logger.info(s"Certificate already issued for: ${event.eData.getOrElse("userId", "")} ${event.related}")
      }
    } catch {
      case e: Exception =>
        metrics.incCounter(config.failedEventCount)
        throw new InvalidEventException(e.getMessage, Map("partition" -> event.partition, "offset" -> event.offset), e)
    }
  }

  @throws[Exception]
  def generateCertificate(event: Event, context: KeyedProcessFunction[String, Event, String]#Context)(implicit metrics: Metrics): Unit = {
    val certModelList: List[CertModel] = new CertMapper(certificateConfig).mapReqToCertModel(event)
    certModelList.foreach(certModel => {
      var uuid: String = null
      val reIssue: Boolean = !event.oldId.isEmpty
      try {
        //prepare the request body for rc create api
        val createCertReq = Map[String, AnyRef](
          JsonKeys.NAME -> certModel.certificateName,
          JsonKeys.CERTIFICATE_NAME -> certModel.certificateName,
          JsonKeys.CERTIFICATE_DESCIPTION -> certModel.certificateDescription,
          JsonKeys.RECIPIENT_NAME -> certModel.recipientName,
          JsonKeys.RECIPIENT_ID -> certModel.identifier
        ) ++ {if (reIssue) Map[String, AnyRef](config.OLD_ID -> event.oldId) else Map[String, AnyRef]()}
        //make api call to registry with identifier
        uuid = callCertificateRc(config.rcCreateApi, null, createCertReq)
        //if reissue then read rc for oldId and call rc delete api
        if(reIssue) {
          try {
            callCertificateRc(config.rcReadApi, event.oldId, null)
            callCertificateRc(config.rcDeleteApi, event.oldId, null)
          } catch {
            case e: ServerException =>
              logger.error("On removing old certificate id exception:: " + e.getMessage, e)
          }
        }
        val related = event.related
        val userEnrollmentData = UserEnrollmentData(related.getOrElse(config.BATCH_ID, "").asInstanceOf[String], certModel.identifier,
          related.getOrElse(config.COURSE_ID, "").asInstanceOf[String], event.courseName, event.templateId,
          Certificate(uuid, event.name, "", formatter.format(new Date())))
        updateUserEnrollmentTable(event, userEnrollmentData, context)
        metrics.incCounter(config.successEventCount)
      } finally {
        cleanUp(uuid, directory)
      }
    })
  }

  @throws[ServerException]
  def callCertificateRc(api: String, identifier: String, request: Map[String, AnyRef]): String = {
    logger.info("Certificate rc called | Api:: " + api)
    var id: String = null
    val status = api match {
      case config.rcDeleteApi => httpUtil.delete(config.rcBaseUrl + "/" + config.rcEntity + "/" + config.rcDeleteApi + "/" +identifier).status
      case config.rcReadApi => httpUtil.get(config.rcBaseUrl + "/" + config.rcEntity + "/" + config.rcReadApi + "/" +identifier, Map[String, String]("Accept"-> "application/vc+ld+json")).status
      case config.rcCreateApi =>
        val httpResponse = httpUtil.post(config.rcBaseUrl + "/" + config.rcEntity + "/" + config.rcCreateApi, ScalaModuleJsonUtils.serialize(request))
        if(httpResponse.status == 200)
          id = httpResponse.body.asInstanceOf[Map[String, AnyRef]].get("result").asInstanceOf[Map[String, AnyRef]].get(config.rcEntity).asInstanceOf[Map[String, AnyRef]].get("osid").asInstanceOf[String]
        httpResponse.status

    }
    if (status == 200) {
      logger.info("certificate rc successfully executed for api: " + api)
    } else {
      logger.error("certificate rc failed for api: " + api +  " | Status is: " + status)
      throw ServerException("ERR_API_CALL", "Something Went Wrong While Making API Call:  " + api +  " | Status is: " + status)
    }
    id
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


  def updateUserEnrollmentTable(event: Event, certMetaData: UserEnrollmentData, context: KeyedProcessFunction[String, Event, String]#Context)(implicit metrics: Metrics): Unit = {
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
        updatedCerts.add(mapAsJavaMap(Map[String, String](
          config.name -> certMetaData.certificate.name,
          config.identifier -> certMetaData.certificate.id,
          config.token -> certMetaData.certificate.token,
        ) ++ {if(!certMetaData.certificate.lastIssuedOn.isEmpty) Map[String, String](config.lastIssuedOn -> certMetaData.certificate.lastIssuedOn)
        else Map[String, String]()}))
        
        val query = getUpdateIssuedCertQuery(updatedCerts, certMetaData.userId, certMetaData.courseId, certMetaData.batchId, config)
        logger.info("update query {}", query.toString)
        val result = cassandraUtil.update(query)
        logger.info("update result {}", result)
        if (result) {
          logger.info("issued certificates in user-enrollment table  updated successfully")
          metrics.incCounter(config.dbUpdateCount)
          val certificateAuditEvent = generateAuditEvent(certMetaData)
          logger.info("pushAuditEvent: audit event generated for certificate : " + certificateAuditEvent)
          val audit = ScalaJsonUtil.serialize(certificateAuditEvent)
          context.output(config.auditEventOutputTag, audit)
          logger.info("pushAuditEvent: certificate audit event success {}", audit)
          context.output(config.notifierOutputTag, NotificationMetaData(certMetaData.userId, certMetaData.courseName, issuedOn, certMetaData.courseId, certMetaData.batchId, certMetaData.templateId, event.partition, event.offset))
          context.output(config.userFeedOutputTag, UserFeedMetaData(certMetaData.userId, certMetaData.courseName, issuedOn, certMetaData.courseId, event.partition, event.offset))
        } else {
          metrics.incCounter(config.failedEventCount)
          throw new Exception(s"Update certificates to enrolments failed: ${event}")
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
      context = EventContext(cdata = Array(Map("type" -> config.courseBatch, config.id -> data.batchId).asJava)),
      `object` = EventObject(id = data.certificate.id, `type` = "Certificate", rollup = Map(config.l1 -> data.courseId).asJava))
  }


}
