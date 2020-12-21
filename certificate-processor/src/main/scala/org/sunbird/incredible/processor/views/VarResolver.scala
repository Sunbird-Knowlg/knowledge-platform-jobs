package org.sunbird.incredible.processor.views

import java.io.UnsupportedEncodingException
import java.net.{URI, URISyntaxException, URLEncoder}
import java.text.{DateFormat, SimpleDateFormat}
import java.util.{Date, Locale}
import com.google.protobuf.TextFormat.ParseException
import org.apache.commons.lang.StringUtils
import org.apache.commons.lang3.ObjectUtils
import org.slf4j.{Logger, LoggerFactory}
import org.sunbird.incredible.JsonKeys
import org.sunbird.incredible.pojos.ob.CertificateExtension


class VarResolver(certificateExtension: CertificateExtension) {

  private val logger: Logger = LoggerFactory.getLogger(classOf[VarResolver])

  def getRecipientName: String = certificateExtension.recipient.name

  def getRecipientId: String = certificateExtension.recipient.identity

  def getCourseName: String = if (ObjectUtils.anyNotNull(certificateExtension.evidence) && StringUtils.isNotBlank(certificateExtension.evidence.name)) certificateExtension.evidence.name
  else ""

  def getQrCodeImage: String = try {
    val uri = new URI(certificateExtension.id)
    val path = uri.getPath
    val idStr = path.substring(path.lastIndexOf('/') + 1)
    StringUtils.substringBefore(idStr, ".") + ".png"
  } catch {
    case e: URISyntaxException =>
      null
  }

  def getIssuedDate: String = {
    val simpleDateFormat: SimpleDateFormat = new SimpleDateFormat(
      "yyyy-MM-dd'T'HH:mm:ss'Z'")
    var dateInFormat: String = null
    try {
      val parsedIssuedDate: Date =
        simpleDateFormat.parse(certificateExtension.issuedOn)
      val format: DateFormat = new SimpleDateFormat("dd MMMM yyy", Locale.getDefault)
      format.format(parsedIssuedDate)
      dateInFormat = format.format(parsedIssuedDate)
    } catch {
      case e: ParseException =>
        logger.info("getIssuedDate: exception occurred while formatting issued date {}", e.getMessage)
    }
    dateInFormat
  }

  def getSignatory0Image: String = if (certificateExtension.signatory.length >= 1) certificateExtension.signatory(0).image
  else ""

  def getSignatory0Designation: String = if (certificateExtension.signatory.length >= 1) certificateExtension.signatory(0).designation
  else ""

  def getSignatory1Image: String = if (certificateExtension.signatory.length >= 2) certificateExtension.signatory(1).image
  else ""

  def getSignatory1Designation: String = if (certificateExtension.signatory.length >= 2) certificateExtension.signatory(1).designation
  else ""

  def getCertificateName: String = certificateExtension.badge.name

  def getCertificateDescription: String = certificateExtension.badge.description

  def getExpiryDate: String = certificateExtension.expires

  def getIssuerName: String = certificateExtension.badge.issuer.name

  @throws[UnsupportedEncodingException]
  def getCertMetaData: java.util.Map[String, String] = {

    val metaData = new java.util.HashMap[String, String]() {
      {
        put(JsonKeys.CERT_NAME, urlEncode(getCertificateName))
        put(JsonKeys.CERTIFICATE_DESCIPTION, urlEncode(getCertificateDescription))
        put(JsonKeys.COURSE_NAME, urlEncode(getCourseName))
        put(JsonKeys.ISSUE_DATE, urlEncode(getIssuedDate))
        put(JsonKeys.RECIPIENT_ID, urlEncode(getRecipientId))
        put(JsonKeys.RECIPIENT_NAME, urlEncode(getRecipientName))
        put(JsonKeys.SIGNATORY_0_IMAGE, urlEncode(getSignatory0Image))
        put(JsonKeys.SIGNATORY_0_DESIGNATION, urlEncode(getSignatory0Designation))
        put(JsonKeys.SIGNATORY_1_IMAGE, urlEncode(getSignatory1Image))
        put(JsonKeys.SIGNATORY_1_DESIGNATION, urlEncode(getSignatory1Designation))
        put(JsonKeys.EXPIRY_DATE, urlEncode(getExpiryDate))
        put(JsonKeys.ISSUER_NAME, urlEncode(getIssuerName))
      }
    }
    metaData
  }

  @throws[UnsupportedEncodingException]
  private def urlEncode(data: String): String = {
    // URLEncoder.encode replace space with "+", but it should %20
    if (StringUtils.isNotEmpty(data)) URLEncoder.encode(data, "UTF-8").replace("+", "%20")
    else data
  }

}
