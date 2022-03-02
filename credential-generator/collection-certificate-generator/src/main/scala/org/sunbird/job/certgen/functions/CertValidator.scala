package org.sunbird.job.certgen.functions

import com.datastax.driver.core.TypeTokens
import com.datastax.driver.core.querybuilder.QueryBuilder

import java.net.{MalformedURLException, URI, URISyntaxException, URL}
import java.text.MessageFormat
import java.util.regex.Pattern
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.sunbird.incredible.JsonKeys
import org.sunbird.job.Metrics
import org.sunbird.job.certgen.domain.Event
import org.sunbird.job.certgen.exceptions.{ErrorCodes, ErrorMessages, ValidationException}
import org.sunbird.job.certgen.task.CertificateGeneratorConfig
import org.sunbird.job.util.CassandraUtil

import scala.collection.JavaConverters._

/**
  * This class contains method to validate certificate api request
  */

class CertValidator() {

  private var publicKeys: List[String] = _
  private val TAG_REGX: String = "[!@#$%^&*()+=,.?/\":;'{}|<>\\s-]"
  private[this] val logger = LoggerFactory.getLogger(classOf[CertValidator])



  /**
    * This method will validate generate certificate request
    *
    */
  @throws[ValidationException]
  def validateGenerateCertRequest(event: Event, enableSuppressException: Boolean): Unit = {
    checkMandatoryParamsPresent(event.eData, "edata", List(JsonKeys.NAME, JsonKeys.SVG_TEMPLATE))
    validateCertData(event.data)
    validateCertIssuer(event.issuer)
    try {
      validateCertSignatoryList(event.signatoryList)
    } catch {
      case e: Exception =>
        if (enableSuppressException) logger.error("SignatoryList Validation failed :: " + e.getMessage, e)
        else throw e
    }
    validateCriteria(event.criteria)
    validateTagId(event.tag)
    val basePath: String = event.basePath
    if (StringUtils.isNotBlank(basePath)) {
      validateBasePath(basePath)
    }
    if (event.keys.nonEmpty) {
      validateKeys(event.keys)
    }
  }

  private def validateCertSignatoryList(signatoryList: List[Map[String, AnyRef]]): Unit = {
    checkMandatoryParamsPresent(signatoryList, JsonKeys.EDATA + "." + JsonKeys.SIGNATORY_LIST, List(JsonKeys.NAME, JsonKeys.ID, JsonKeys.DESIGNATION,
      JsonKeys.SIGNATORY_IMAGE))
  }

  private def validateCertIssuer(issuer: Map[String, AnyRef]): Unit = {
    checkMandatoryParamsPresent(issuer, JsonKeys.EDATA + "." + JsonKeys.ISSUER, List(JsonKeys.NAME, JsonKeys.URL))
    publicKeys = issuer.getOrElse(JsonKeys.PUBLIC_KEY, List[String]()).asInstanceOf[List[String]]
  }

  private def validateCriteria(criteria: Map[String, AnyRef]): Unit = {
    checkMandatoryParamsPresent(criteria, JsonKeys.EDATA + "." + JsonKeys.CRITERIA, List(JsonKeys.NARRATIVE))
  }

  private def validateCertData(data: List[Map[String, AnyRef]]): Unit = {
    checkMandatoryParamsPresent(data, JsonKeys.EDATA + "." + JsonKeys.DATA, List(JsonKeys.RECIPIENT_NAME))
  }

  private def validateKeys(keys: Map[String, AnyRef]): Unit = {
    checkMandatoryParamsPresent(keys, JsonKeys.EDATA + "." + JsonKeys.KEYS, List(JsonKeys.ID))
    if (publicKeys.nonEmpty) {
      validateIssuerPublicKeys(keys)
    }
  }

  /**
    * this method used to validate public keys of Issuer object ,if  public key list is present, list  must contain keys.id value of
    * certificate request
    *
    * @param
    */
  private def validateIssuerPublicKeys(keys: Map[String, AnyRef]): Unit = {
    val keyIds = List[String]()
    publicKeys.foreach(publicKey =>
      if (publicKey.startsWith("http")) {
        keyIds :+ getKeyIdFromPublicKeyUrl(publicKey)
      } else {
        keyIds :+ publicKey
      })
    if (!keyIds.contains(keys.getOrElse(JsonKeys.ID, ""))) {
      throw ValidationException(
        ErrorCodes.INVALID_PARAM_VALUE,
        MessageFormat.format(ErrorMessages.INVALID_PARAM_VALUE, publicKeys, JsonKeys.EDATA + "." + JsonKeys.ISSUER + "." + JsonKeys.PUBLIC_KEY) +
          " ,public key attribute must contain keys.id value")
    }
  }

  /**
    * to get keyId from the publicKey url
    *
    * @return
    */
  private def getKeyIdFromPublicKeyUrl(publicKey: String): String = {
    var idStr: String = null
    try {
      val uri: URI = new URI(publicKey)
      val path: String = uri.getPath
      idStr = path.substring(path.lastIndexOf('/') + 1)
      idStr = idStr.substring(0, 1)
    } catch {
      case e: URISyntaxException =>
    }
    idStr
  }

  @throws[ValidationException]
  private def checkMandatoryParamsPresent(data: Map[String, AnyRef], parentKey: String, keys: List[String]): Unit = {
    if (data.isEmpty) throw ValidationException(ErrorCodes.MANDATORY_PARAMETER_MISSING, MessageFormat.format(ErrorMessages.MANDATORY_PARAMETER_MISSING, parentKey))
    checkChildrenMapMandatoryParams(data, keys, parentKey)
  }

  private def checkMandatoryParamsPresent(dataList: List[Map[String, AnyRef]], parentKey: String, keys: List[String]): Unit = {
    if (dataList.isEmpty) {
      throw ValidationException(ErrorCodes.MANDATORY_PARAMETER_MISSING, MessageFormat.format(ErrorMessages.MANDATORY_PARAMETER_MISSING, parentKey))
    }
    dataList.foreach(data => checkChildrenMapMandatoryParams(data, keys, parentKey))
  }

  @throws[ValidationException]
  private def checkChildrenMapMandatoryParams(data: Map[String, AnyRef], keys: List[String], parentKey: String): Unit = {
    keys.foreach(key => {
      if (StringUtils.isBlank(data.getOrElse(key, "").toString))
        throw ValidationException(ErrorCodes.MANDATORY_PARAMETER_MISSING, MessageFormat.format(ErrorMessages.MANDATORY_PARAMETER_MISSING, parentKey + "." + key))
    })

  }

  @throws[ValidationException]
  private def validateBasePath(basePath: String): Unit = {
    val isValid: Boolean = isUrlValid(basePath)
    if (!isValid) {
      throw ValidationException(ErrorCodes.INVALID_PARAM_VALUE, MessageFormat.format(ErrorMessages.INVALID_PARAM_VALUE, basePath, JsonKeys.CERTIFICATE + "." + JsonKeys.BASE_PATH))
    }
  }


  private def isUrlValid(url: String): Boolean = try {
    val obj: URL = new URL(url)
    obj.toURI
    true
  } catch {
    case e: MalformedURLException =>
      false
    case e: URISyntaxException =>
      false
  }

  /**
    * validates tagId , if tagId contains any special character except '_' then tagId is invalid
    *
    */
  @throws[ValidationException]
  private def validateTagId(tag: String): Unit = {
    if (StringUtils.isNotBlank(tag)) {
      val pattern = Pattern.compile(TAG_REGX)
      if (pattern.matcher(tag).find()) {
        throw ValidationException(ErrorCodes.INVALID_PARAM_VALUE, MessageFormat.format(ErrorMessages.INVALID_PARAM_VALUE, tag, JsonKeys.TAG))
      }
    }
  }
  
  def isNotIssued(event: Event)(config: CertificateGeneratorConfig, metrics: Metrics, cassandraUtil: CassandraUtil):Boolean = {
    val query = QueryBuilder.select( "issued_certificates").from(config.dbKeyspace, config.dbEnrollmentTable)
      .where(QueryBuilder.eq(config.dbUserId, event.eData.getOrElse("userId", "")))
      .and(QueryBuilder.eq(config.dbCourseId, event.related.getOrElse("courseId", "")))
      .and(QueryBuilder.eq(config.dbBatchId, event.related.getOrElse("batchId", "")))
    val row = cassandraUtil.findOne(query.toString)
    metrics.incCounter(config.enrollmentDbReadCount)
    if (null != row) {
      val issuedCertificates = row
        .getList(config.issuedCertificates, TypeTokens.mapOf(classOf[String], classOf[String])).asScala.toList
      val isCertIssued = !issuedCertificates.isEmpty && !issuedCertificates
        .filter(cert => event.name.equalsIgnoreCase(cert.getOrDefault(config.name, "").asInstanceOf[String])).isEmpty
      ((null != event.oldId && !event.oldId.isEmpty) || !isCertIssued)
    } else false
  }

}
