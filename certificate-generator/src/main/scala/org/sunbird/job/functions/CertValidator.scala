package org.sunbird.job.functions


import org.apache.commons.collections.{CollectionUtils, MapUtils}
import org.apache.commons.lang3.StringUtils
import java.net.{MalformedURLException, URI, URL}
import java.util
import java.util.Arrays
import java.util.Map
import java.util.regex.Pattern
import java.net.URISyntaxException
import java.text.MessageFormat

import org.sunbird.incredible.processor.JsonKey
import org.sunbird.job.Exceptions.{ErrorCodes, ErrorMessages, ValidationException}

/**
  * This class contains method to validate certificate api request
  */

class CertValidator {

  private var publicKeys: util.List[String] = _
  private val TAG_REGX: String = "[!@#$%^&*()+=,.?/\":;'{}|<>\\s-]"


  /**
    * This method will validate generate certificate request
    *
    */
  @throws[ValidationException]
  def validateGenerateCertRequest(certReq: util.Map[String, AnyRef]): Unit = {
    checkMandatoryParamsPresent(certReq, JsonKey.CERTIFICATE, util.Arrays.asList(JsonKey.NAME, JsonKey.SVG_TEMPLATE))
    validateCertData(certReq.get(JsonKey.DATA).asInstanceOf[util.List[util.Map[String, AnyRef]]])
    validateCertIssuer(certReq.get(JsonKey.ISSUER).asInstanceOf[util.Map[String, AnyRef]])
    validateCertSignatoryList(certReq.get(JsonKey.SIGNATORY_LIST).asInstanceOf[util.List[util.Map[String, AnyRef]]])
    validateCriteria(certReq.get(JsonKey.CRITERIA).asInstanceOf[util.Map[String, AnyRef]])
    validateTagId(certReq.get(JsonKey.TAG).asInstanceOf[String])
    val basePath: String = certReq.get(JsonKey.BASE_PATH).asInstanceOf[String]
    if (StringUtils.isNotBlank(basePath)) {
      validateBasePath(basePath)
    }
    if (certReq.containsKey(JsonKey.KEYS)) {
      validateKeys(certReq.get(JsonKey.KEYS).asInstanceOf[util.Map[String, AnyRef]])
    }
  }

  private def validateCertSignatoryList(signatoryList: util.List[util.Map[String, AnyRef]]): Unit = {
    checkMandatoryParamsPresent(signatoryList, JsonKey.CERTIFICATE + "." + JsonKey.SIGNATORY_LIST, util.Arrays.asList(JsonKey.NAME, JsonKey.ID, JsonKey.DESIGNATION,
      JsonKey.SIGNATORY_IMAGE))
  }

  private def validateCertIssuer(issuer: util.Map[String, AnyRef]): Unit = {
    checkMandatoryParamsPresent(issuer, JsonKey.CERTIFICATE + "." + JsonKey.ISSUER, Arrays.asList(JsonKey.NAME, JsonKey.URL))
    publicKeys = issuer.get(JsonKey.PUBLIC_KEY).asInstanceOf[util.List[String]]
  }

  private def validateCriteria(criteria: Map[String, AnyRef]): Unit = {
    checkMandatoryParamsPresent(criteria, JsonKey.CERTIFICATE + "." + JsonKey.CRITERIA, Arrays.asList(JsonKey.NARRATIVE))
  }

  private def validateCertData(data: util.List[util.Map[String, AnyRef]]): Unit = {
    checkMandatoryParamsPresent(data, JsonKey.CERTIFICATE + "." + JsonKey.DATA, Arrays.asList(JsonKey.RECIPIENT_NAME))
  }

  private def validateKeys(keys: Map[String, AnyRef]): Unit = {
    checkMandatoryParamsPresent(keys, JsonKey.CERTIFICATE + "." + JsonKey.KEYS, Arrays.asList(JsonKey.ID))
    if (CollectionUtils.isNotEmpty(publicKeys)) {
      validateIssuerPublicKeys(keys)
    }
  }

  /**
    * this method used to validate public keys of Issuer object ,if  public key list is present, list  must contain keys.id value of
    * certificate request
    *
    * @param
    */
  private def validateIssuerPublicKeys(keys: util.Map[String, AnyRef]): Unit = {
    val keyIds: util.List[String] = new util.ArrayList[String]()
    publicKeys.forEach(publicKey =>
      if (publicKey.startsWith("http")) {
        keyIds.add(getKeyIdFromPublicKeyUrl(publicKey))
      } else {
        keyIds.add(publicKey)
      })
    if (!keyIds.contains(keys.get(JsonKey.ID))) {
      throw ValidationException(
        ErrorCodes.INVALID_PARAM_VALUE,
        MessageFormat.format(ErrorMessages.INVALID_PARAM_VALUE, publicKeys, JsonKey.CERTIFICATE + "." + JsonKey.ISSUER + "." + JsonKey.PUBLIC_KEY) +
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
  private def checkMandatoryParamsPresent(data: util.Map[String, AnyRef], parentKey: String, keys: util.List[String]): Unit = {
    if (MapUtils.isEmpty(data)) throw ValidationException(ErrorCodes.MANDATORY_PARAMETER_MISSING, MessageFormat.format(ErrorMessages.MANDATORY_PARAMETER_MISSING, parentKey))
    checkChildrenMapMandatoryParams(data, keys, parentKey)
  }

  private def checkMandatoryParamsPresent(dataList: util.List[util.Map[String, AnyRef]], parentKey: String, keys: util.List[String]): Unit = {
    if (CollectionUtils.isEmpty(dataList)) {
      throw ValidationException(ErrorCodes.MANDATORY_PARAMETER_MISSING, MessageFormat.format(ErrorMessages.MANDATORY_PARAMETER_MISSING, parentKey))
    }
    dataList.forEach(data => checkChildrenMapMandatoryParams(data, keys, parentKey))

  }

  @throws[ValidationException]
  private def checkChildrenMapMandatoryParams(data: util.Map[String, AnyRef], keys: util.List[String], parentKey: String): Unit = {
    keys.forEach(key => {
      if (StringUtils.isBlank(data.get(key).asInstanceOf[String]))
        throw ValidationException(ErrorCodes.MANDATORY_PARAMETER_MISSING, MessageFormat.format(ErrorMessages.MANDATORY_PARAMETER_MISSING, parentKey + "." + key))

    })

  }

  @throws[ValidationException]
  private def validateBasePath(basePath: String): Unit = {
    val isValid: Boolean = isUrlValid(basePath)
    if (!isValid) {
      throw ValidationException(ErrorCodes.INVALID_PARAM_VALUE, MessageFormat.format(ErrorMessages.INVALID_PARAM_VALUE, basePath, JsonKey.CERTIFICATE + "." + JsonKey.BASE_PATH))
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
        throw ValidationException(ErrorCodes.INVALID_PARAM_VALUE, MessageFormat.format(ErrorMessages.INVALID_PARAM_VALUE, tag, JsonKey.TAG))
      }
    }
  }

}
