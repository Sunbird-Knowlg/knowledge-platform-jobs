package org.sunbird.job.functions

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.commons.lang3.StringUtils
import org.sunbird.incredible.{CertificateConfig, JsonKeys}
import org.sunbird.incredible.pojos.ob.{Criteria, Issuer, SignatoryExtension}
import org.sunbird.incredible.pojos.valuator.{ExpiryDateValuator, IssuedDateValuator}
import org.sunbird.incredible.processor.CertModel
import org.sunbird.job.domain.Event

import scala.collection.JavaConverters._


class CertMapper(certConfig: CertificateConfig) {

  def mapReqToCertModel(certReq: Event): List[CertModel] = {
    val dataList: List[Map[String, AnyRef]] = certReq.data
    val signatoryArr = getSignatoryArray(certReq.signatoryList)
    val issuedDate = new IssuedDateValuator().evaluates(if (StringUtils.isBlank(certReq.issuedDate)) getCurrentDate else certReq.issuedDate)
    val expiryDate: String = if (StringUtils.isNotBlank(certReq.expiryDate)) new ExpiryDateValuator(issuedDate).evaluates(certReq.expiryDate) else ""
    val certList: List[CertModel] = dataList.toStream.map((data: Map[String, AnyRef]) => {
      val certModel: CertModel = CertModel(recipientName = data.getOrElse(JsonKeys.RECIPIENT_NAME, "").asInstanceOf[String],
        recipientEmail = Option.apply(data.getOrElse(JsonKeys.RECIPIENT_EMAIl, "").asInstanceOf[String]),
        recipientPhone = Option.apply(data.getOrElse(JsonKeys.RECIPIENT_PHONE, "").asInstanceOf[String]),
        identifier = data.getOrElse(JsonKeys.RECIPIENT_ID, "").asInstanceOf[String],
        validFrom = Option.apply(data.getOrElse(JsonKeys.VALID_FROM, null).asInstanceOf[String]),
        issuer = getIssuer(certReq),
        courseName = certReq.courseName,
        issuedDate = issuedDate,
        certificateLogo = Option.apply(certReq.logo),
        certificateDescription = Option.apply(certReq.certificateDescription),
        certificateName = certReq.name,
        signatoryList = signatoryArr.toArray,
        criteria = getCriteria(certReq.criteria),
        keyId = certReq.keys.getOrElse(JsonKeys.ID, ""),
        tag = certReq.tag,
        expiry = Option.apply(expiryDate)
      )
      certModel
    }).toList

    certList
  }

  private def getCurrentDate: String = {
    val dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'")
    dtf.format(LocalDateTime.now)
  }

  private def getCriteria(criteriaData: Map[String, String]): Criteria = Criteria(narrative = criteriaData.getOrElse(JsonKeys.NARRATIVE, ""))


  private def getIssuer(req: Event): Issuer = {
    val issuerData: Map[String, AnyRef] = req.issuer
    val publicKeys: List[String] = getPublicKeys(issuerData.getOrElse(JsonKeys.PUBLIC_KEY, new java.util.ArrayList[String]()).asInstanceOf[java.util.ArrayList[String]].asScala.toList, req.keys)
    val issuer: Issuer = Issuer(context = certConfig.contextUrl, name = issuerData.getOrElse(JsonKeys.NAME, "").asInstanceOf[String], url =
      issuerData.getOrElse(JsonKeys.URL, "").asInstanceOf[String], publicKey = Option.apply(publicKeys.toArray))
    issuer
  }


  private def getSignatoryArray(signatoryList: List[Map[String, AnyRef]]): List[SignatoryExtension] =
    signatoryList.toStream.map((signatory: Map[String, AnyRef]) =>
      getSignatory(signatory)).toList


  private def getSignatory(signatory: Map[String, AnyRef]): SignatoryExtension = {
    SignatoryExtension(context = certConfig.signatoryExtension, identity =
      signatory.getOrElse(JsonKeys.ID, "").asInstanceOf[String],
      designation = signatory.getOrElse(JsonKeys.DESIGNATION, "").asInstanceOf[String],
      image = signatory.getOrElse(JsonKeys.SIGNATORY_IMAGE, "").asInstanceOf[String],
      name = signatory.getOrElse(JsonKeys.NAME, "").asInstanceOf[String])
  }

  private def getPublicKeys(issuerPublicKeys: List[String], keys: Map[String, AnyRef]): List[String] = {
    var publicKeys = issuerPublicKeys
    if (issuerPublicKeys.isEmpty && keys.nonEmpty) {
      publicKeys = List()
      publicKeys :+ keys.get(JsonKeys.ID).asInstanceOf[String]
    }
    val validatedPublicKeys: List[String] = List()
    if (publicKeys.nonEmpty)
      publicKeys.foreach((publicKey: String) => {
        if (!publicKey.startsWith("http"))
          validatedPublicKeys :+ (certConfig.basePath.concat("/") + JsonKeys.KEYS.concat("/") + publicKey.concat("_publicKey.json"))
        else validatedPublicKeys :+ publicKey
      })
    validatedPublicKeys
  }


}

