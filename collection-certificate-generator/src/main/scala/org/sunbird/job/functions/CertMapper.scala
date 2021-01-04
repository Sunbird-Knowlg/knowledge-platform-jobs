package org.sunbird.job.functions

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.stream.Collectors

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.commons.lang3.StringUtils
import org.sunbird.incredible.{CertificateConfig, JsonKeys}
import org.sunbird.incredible.pojos.ob.{Criteria, Issuer, SignatoryExtension}
import org.sunbird.incredible.pojos.valuator.{ExpiryDateValuator, IssuedDateValuator}
import org.sunbird.incredible.processor.CertModel
import org.sunbird.job.domain.Event

import scala.collection.JavaConverters._


class CertMapper(certConfig: CertificateConfig) {

  lazy private val mapper: ObjectMapper = new ObjectMapper()

  def mapReqToCertModel(certReq: Event): List[CertModel] = {
    val dataList: List[Map[String, AnyRef]] = certReq.data
    val signatoryArr = getSignatoryArray(certReq.signatoryList)
    val issuedDate = new IssuedDateValuator().evaluates(if (StringUtils.isBlank(certReq.issuedDate)) getCurrentDate else certReq.issuedDate)
    val certList: List[CertModel] = dataList.toStream.map((data: Map[String, AnyRef]) => {
      val certModel: CertModel = CertModel(recipientName = data.get(JsonKeys.RECIPIENT_NAME).asInstanceOf[String],
        recipientEmail = Option.apply(data.get(JsonKeys.RECIPIENT_EMAIl).asInstanceOf[String]),
        recipientPhone = Option.apply(data.get(JsonKeys.RECIPIENT_PHONE).asInstanceOf[String]),
        identifier = data.get(JsonKeys.RECIPIENT_ID).asInstanceOf[String],
        validFrom = Option.apply(data.get(JsonKeys.VALID_FROM).asInstanceOf[String]),
        issuer = getIssuer(certReq),
        courseName = certReq.courseName,
        issuedDate = issuedDate,
        certificateLogo = Option.apply(certReq.logo),
        certificateDescription = Option.apply(certReq.certificateDescription),
        certificateName = certReq.name,
        signatoryList = signatoryArr.toArray,
        criteria = getCriteria(certReq.criteria)
      )
      certModel
    }).toList

    certList
  }

  private def getCurrentDate: String = {
    val dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'")
    dtf.format(LocalDateTime.now)
  }

  private def getCriteria(criteriaData: Map[String, AnyRef]): Criteria = mapper.convertValue(criteriaData, classOf[Criteria])


  private def getIssuer(req: Event): Issuer = {
    val issuerData: Map[String, AnyRef] = req.issuer
    val publicKeys: List[String] = getPublicKeys(issuerData.get(JsonKeys.PUBLIC_KEY).asInstanceOf[List[String]], req.keys)
    val issuer: Issuer = Issuer(context = certConfig.contextUrl, name = issuerData.get(JsonKeys.NAME).asInstanceOf[String], url =
      issuerData.get(JsonKeys.URL).asInstanceOf[String], publicKey = Option.apply(publicKeys.toArray))
    issuer
  }


  private def getSignatoryArray(signatoryList: List[Map[String, AnyRef]]): List[SignatoryExtension] =
    signatoryList.toStream.map((signatory: Map[String, AnyRef]) =>
      getSignatory(signatory)).toList


  private def getSignatory(signatory: Map[String, AnyRef]): SignatoryExtension = {
    SignatoryExtension(context = certConfig.signatoryExtension, identity = signatory.get(JsonKeys.ID).asInstanceOf[String]
      , designation = signatory.get(JsonKeys.DESIGNATION).asInstanceOf[String], image = signatory.get(JsonKeys.SIGNATORY_IMAGE).asInstanceOf[String], name = "")
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

