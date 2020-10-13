package org.sunbird.job

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.{List, Map}
import java.util.stream.Collectors

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.commons.collections.{CollectionUtils, MapUtils}
import org.apache.commons.lang3.StringUtils
import org.sunbird.incredible.pojos.SignatoryExtension
import org.sunbird.incredible.pojos.ob.{Criteria, Issuer}
import org.sunbird.incredible.processor.{CertModel, JsonKey}


class CertMapper(properties: java.util.Map[String, String]) {

  lazy private val mapper: ObjectMapper = new ObjectMapper()

  def mapReqToCertModel(certReq: java.util.Map[String, AnyRef]): java.util.List[CertModel] = {
    println("properties " + properties)
    val dataList: java.util.List[java.util.Map[String, AnyRef]] = certReq.get(JsonKey.DATA).asInstanceOf[java.util.List[java.util.Map[String, AnyRef]]]
    val certList: java.util.List[CertModel] = dataList.stream().map[CertModel]((data: java.util.Map[String, AnyRef]) => getCertModel(data)).collect(Collectors.toList())
    val issuerData: java.util.Map[String, AnyRef] = certReq.get(JsonKey.ISSUER).asInstanceOf[java.util.Map[String, AnyRef]]
    val issuer: Issuer = getIssuer(issuerData)
    val publicKeys: java.util.List[String] = validatePublicKeys(issuerData.get(JsonKey.PUBLIC_KEY).asInstanceOf[List[String]], certReq.get(JsonKey.KEYS).asInstanceOf[Map[String, AnyRef]])
    issuer.setPublicKey(publicKeys.toArray(new Array[String](0)))
    val signatoryArr: java.util.List[SignatoryExtension] = getSignatoryArray(certReq.get(JsonKey.SIGNATORY_LIST).asInstanceOf[java.util.List[java.util.Map[String, AnyRef]]])
    val criteria: Criteria = getCriteria(certReq.get(JsonKey.CRITERIA).asInstanceOf[java.util.Map[String, AnyRef]])
    certList.forEach((cert: CertModel) => {
      cert.setIssuer(issuer)
      cert.setSignatoryList(signatoryArr.toArray(Array.ofDim[SignatoryExtension](signatoryArr.size)))
      cert.setCourseName(certReq.get(JsonKey.COURSE_NAME).asInstanceOf[String])
      cert.setCertificateDescription(certReq.get(JsonKey.DESCRIPTION).asInstanceOf[String])
      cert.setCertificateLogo(certReq.get(JsonKey.LOGO).asInstanceOf[String])
      cert.setCriteria(criteria)
      val issuedDate = certReq.get(JsonKey.ISSUE_DATE).asInstanceOf[String]
      if (StringUtils.isBlank(issuedDate)) cert.setIssuedDate(getCurrentDate)
      else cert.setIssuedDate(certReq.get(JsonKey.ISSUE_DATE).asInstanceOf[String])
      cert.setCertificateName(certReq.get(JsonKey.CERTIFICATE_NAME).asInstanceOf[String])
    })
    certList
  }

  private def getCurrentDate: String = {
    val dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'")
    dtf.format(LocalDateTime.now)
  }

  private def getCriteria(criteriaData: java.util.Map[String, AnyRef]): Criteria = mapper.convertValue(criteriaData, classOf[Criteria])


  private def getCertModel(data: java.util.Map[String, AnyRef]): CertModel = {
    val certModel = new CertModel
    certModel.setRecipientName(data.get(JsonKey.RECIPIENT_NAME).asInstanceOf[String])
    certModel.setRecipientEmail(data.get(JsonKey.RECIPIENT_EMAIl).asInstanceOf[String])
    certModel.setRecipientPhone(data.get(JsonKey.RECIPIENT_PHONE).asInstanceOf[String])
    certModel.setIdentifier(data.get(JsonKey.RECIPIENT_ID).asInstanceOf[String])
    certModel.setValidFrom(data.get(JsonKey.VALID_FROM).asInstanceOf[String])
    certModel.setExpiry(data.get(JsonKey.EXPIRY).asInstanceOf[String])
    certModel
  }

  private def getIssuer(issuerData: java.util.Map[String, AnyRef]): Issuer = {
    val issuer = new Issuer(properties.get(JsonKey.CONTEXT))
    issuer.setName(issuerData.get(JsonKey.NAME).asInstanceOf[String])
    issuer.setUrl(issuerData.get(JsonKey.URL).asInstanceOf[String])
    issuer
  }


  private def getSignatoryArray(signatoryList: java.util.List[java.util.Map[String, AnyRef]]): java.util.List[SignatoryExtension] =
    signatoryList.stream().map[SignatoryExtension]((signatory: java.util.Map[String, AnyRef]) => getSignatory(signatory)).collect(Collectors.toList())


  private def getSignatory(signatory: java.util.Map[String, AnyRef]): SignatoryExtension = {
    val signatoryExtension = new SignatoryExtension(properties.get(JsonKey.SIGNATORY_EXTENSION))
    signatoryExtension.setIdentity(signatory.get(JsonKey.ID).asInstanceOf[String])
    signatoryExtension.setDesignation(signatory.get(JsonKey.DESIGNATION).asInstanceOf[String])
    signatoryExtension.setImage(signatory.get(JsonKey.SIGNATORY_IMAGE).asInstanceOf[String])
    signatoryExtension
  }

  private def validatePublicKeys(publicKeys: java.util.List[String], keys: java.util.Map[String, AnyRef]): java.util.List[String] = {
    if (CollectionUtils.isEmpty(publicKeys) && MapUtils.isNotEmpty(keys)) {
      publicKeys.add(keys.get(JsonKey.ID).asInstanceOf[String])
    }
    val validatedPublicKeys: java.util.List[String] = new java.util.ArrayList[String]
    if (CollectionUtils.isNotEmpty(publicKeys))
      publicKeys.forEach((publicKey: String) => {
      if (!publicKey.startsWith("http"))
        validatedPublicKeys.add(properties.get(JsonKey.BASE_PATH).concat("/") + JsonKey.KEYS.concat("/") + publicKey.concat("_publicKey.json"))
      else validatedPublicKeys.add(publicKey)
    })
    validatedPublicKeys
  }


}

