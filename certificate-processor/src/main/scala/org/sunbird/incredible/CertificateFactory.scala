package org.sunbird.incredible

import java.io.IOException
import java.time.Instant
import java.util.UUID

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.ObjectMapper
import com.twitter.storehaus.cache.{Cache, LRUCache}
import org.apache.commons.lang.StringUtils
import org.slf4j.{Logger, LoggerFactory}
import org.sunbird.incredible.pojos.ob.{BadgeClass, CertificateExtension, CompositeIdentityObject, Issuer, Signature, SignedVerification, TrainingEvidence}
import org.sunbird.incredible.processor.CertModel
import org.sunbird.incredible.processor.signature.{SignatureException, SignatureHelper}

class CertificateFactory(properties: Map[String, String]) {

  private val logger: Logger = LoggerFactory.getLogger(classOf[CertificateFactory])
  lazy private val mapper: ObjectMapper = new ObjectMapper()
  mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL)

  def createCertificate(certModel: CertModel): CertificateExtension = {
    val basePath = getDomainUrl
    val uuid: String = basePath + "/" + UUID.randomUUID.toString
    val compositeIdentity: CompositeIdentityObject = CompositeIdentityObject(context = properties(JsonKeys.CONTEXT), identity = certModel.identifier, name = certModel.recipientName, hashed = false, `type` = Array(JsonKeys.ID))
    val issuer: Issuer = Issuer(context = properties(JsonKeys.CONTEXT), id = properties(JsonKeys.ISSUER_URL), name = certModel.issuer.name, url = certModel.issuer.url, publicKey = certModel.issuer.publicKey)
    val badgeClass: BadgeClass = BadgeClass(properties(JsonKeys.CONTEXT),
      id = properties(JsonKeys.BADGE_URL),
      description = certModel.certificateDescription,
      name = if (StringUtils.isNotEmpty(certModel.courseName)) certModel.courseName else certModel.certificateName,
      image = certModel.certificateLogo, issuer = issuer, criteria = certModel.criteria)

    val certificateExtension: CertificateExtension = CertificateExtension(properties(JsonKeys.CONTEXT), id = uuid, recipient = compositeIdentity
      , badge = badgeClass, issuedOn = certModel.issuedDate, expires = certModel.expiry, validFrom = certModel.validFrom, signatory = certModel.signatoryList)
    if (StringUtils.isNotEmpty(certModel.courseName)) {
      val trainingEvidence: TrainingEvidence = TrainingEvidence(properties(JsonKeys.CONTEXT), id = properties(JsonKeys.EVIDENCE_URL), name = certModel.courseName)
      certificateExtension.evidence = Option.apply(trainingEvidence)
    }

    var signedVerification: SignedVerification = null
    if (StringUtils.isEmpty(properties.get(JsonKeys.KEY_ID).getOrElse(""))) {
      signedVerification = SignedVerification(`type` = Array(JsonKeys.HOSTED))
      logger.info("CertificateExtension:createCertificate: if keyID is empty then verification type is HOSTED")
    } else {
      signedVerification = SignedVerification(creator = Option.apply(properties(JsonKeys.PUBLIC_KEY_URL)))
      logger.info("CertificateExtension:createCertificate: if keyID is not empty then verification type is SignedBadge")
      val signatureValue = getSignatureValue(certificateExtension)
      val signature: Signature = Signature(created = Instant.now.toString, creator = properties(JsonKeys.SIGN_CREATOR), signatureValue = signatureValue)
      certificateExtension.signature = Option.apply(signature)
    }
    certificateExtension
  }


  /**
    * to get signature value of certificate
    *
    * @return
    */
  @throws[IOException]
  private def getSignatureValue(certificateExtension: CertificateExtension): String = {
    val signatureHelper = new SignatureHelper(properties(JsonKeys.ENC_SERVICE_URL))
    var signMap: Map[String, AnyRef] = null
    mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL)
    val request = mapper.writeValueAsString(certificateExtension)
    val jsonNode = mapper.readTree(request)
    logger.info("CertificateFactory:getSignatureValue:Json node of certificate".concat(jsonNode.toString))
    signMap = signatureHelper.generateSignature(jsonNode, properties(JsonKeys.KEY_ID))
    signMap.get(JsonKeys.SIGNATURE_VALUE).asInstanceOf[String]
  }

  /**
    * appends slug , org id, batch id to the domain url
    */
  private def getDomainUrl: String = {
    val stringBuilder: StringBuilder = new StringBuilder
    stringBuilder.append(properties.get(JsonKeys.BASE_PATH))
    if (StringUtils.isNotEmpty(properties(JsonKeys.TAG))) stringBuilder.append("/" + properties.get(JsonKeys.TAG))
    stringBuilder.toString
  }

}
