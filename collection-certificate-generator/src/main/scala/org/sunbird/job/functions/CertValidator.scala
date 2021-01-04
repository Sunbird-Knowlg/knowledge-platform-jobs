package org.sunbird.job.functions

import org.apache.commons.lang3.StringUtils
import java.net.{MalformedURLException, URI, URL}
import java.util.Arrays
import java.util.regex.Pattern
import java.net.URISyntaxException
import java.text.MessageFormat

import org.sunbird.incredible.JsonKeys
import org.sunbird.job.Exceptions.{ErrorCodes, ErrorMessages, ValidationException}
import org.sunbird.job.domain.Event

/**
  * This class contains method to validate certificate api request
  */

class CertValidator {

  private var publicKeys: List[String] = _
  private val TAG_REGX: String = "[!@#$%^&*()+=,.?/\":;'{}|<>\\s-]"


  /**
    * This method will validate generate certificate request
    *
    */
  @throws[ValidationException]
  def validateGenerateCertRequest(event: Event): Unit = {
    checkMandatoryParamsPresent(event.eData, "edata", List(JsonKeys.NAME, JsonKeys.SVG_TEMPLATE))
    validateCertData(event.data)
    validateCertIssuer(event.issuer)
    validateCertSignatoryList(event.signatoryList)
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
    checkMandatoryParamsPresent(signatoryList, JsonKeys.CERTIFICATE + "." + JsonKeys.SIGNATORY_LIST, List(JsonKeys.NAME, JsonKeys.ID, JsonKeys.DESIGNATION,
      JsonKeys.SIGNATORY_IMAGE))
  }

  private def validateCertIssuer(issuer: Map[String, AnyRef]): Unit = {
    checkMandatoryParamsPresent(issuer, JsonKeys.CERTIFICATE + "." + JsonKeys.ISSUER, List(JsonKeys.NAME, JsonKeys.URL))
    publicKeys = issuer.get(JsonKeys.PUBLIC_KEY).asInstanceOf[List[String]]
  }

  private def validateCriteria(criteria: Map[String, AnyRef]): Unit = {
    checkMandatoryParamsPresent(criteria, JsonKeys.CERTIFICATE + "." + JsonKeys.CRITERIA, List(JsonKeys.NARRATIVE))
  }

  private def validateCertData(data: List[Map[String, AnyRef]]): Unit = {
    checkMandatoryParamsPresent(data, JsonKeys.CERTIFICATE + "." + JsonKeys.DATA, List(JsonKeys.RECIPIENT_NAME))
  }

  private def validateKeys(keys: Map[String, AnyRef]): Unit = {
    checkMandatoryParamsPresent(keys, JsonKeys.CERTIFICATE + "." + JsonKeys.KEYS, List(JsonKeys.ID))
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
    if (!keyIds.contains(keys.get(JsonKeys.ID))) {
      throw ValidationException(
        ErrorCodes.INVALID_PARAM_VALUE,
        MessageFormat.format(ErrorMessages.INVALID_PARAM_VALUE, publicKeys, JsonKeys.CERTIFICATE + "." + JsonKeys.ISSUER + "." + JsonKeys.PUBLIC_KEY) +
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
      if (StringUtils.isBlank(data.get(key).asInstanceOf[String]))
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

}
