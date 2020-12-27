package org.sunbird.incredible.processor.signature

import java.io.IOException

import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.http.client.ClientProtocolException
import org.slf4j.{Logger, LoggerFactory}
import org.sunbird.incredible.{HTTPResponse, HttpUtil, JsonKeys}


object SignatureHelper {

  var httpUtil = new HttpUtil
  private val logger: Logger = LoggerFactory.getLogger(getClass.getName)
  lazy private val mapper: ObjectMapper = new ObjectMapper()

  /**
    * This method calls signature service for signing the object
    *
    * @param rootNode - contains input need to be signed
    * @return - signed data with key
    * @throws SignatureException.UnreachableException
    * @throws SignatureException.CreationException
    */

  def generateSignature(rootNode: JsonNode, keyId: String)(implicit encServiceUrl: String): java.util.Map[String, AnyRef] = {
    val signReq: Map[String, AnyRef] = Map(JsonKeys.ENTITY -> rootNode)
    logger.info("generateSignature:keyID:".concat(keyId))
    val signApiURl: String = encServiceUrl.concat("/" + JsonKeys.SIGN + "/").concat(keyId)
    logger.info("generateSignature:enc service url formed:".concat(signApiURl))
    try {
      logger.info("generateSignature:SignRequest for enc-service call:".concat(mapper.writeValueAsString(signReq)))
      val response = httpUtil.post(signApiURl, mapper.writeValueAsString(signReq))
      mapper.readValue(response.body, new TypeReference[java.util.Map[String, AnyRef]]() {})
    } catch {
      case e: ClientProtocolException =>
        logger.error("ClientProtocolException when signing: {}", e.getMessage)
        throw new SignatureException.UnreachableException(e.getMessage)
      case e: IOException =>
        logger.error("RestClientException when signing: {}", e.getMessage)
        throw new SignatureException.CreationException(e.getMessage)
    }
  }


  def verifySignature(rootNode: JsonNode)(implicit encServiceUrl: String): Boolean = {
    val verifyEndPoint = encServiceUrl.concat("/" + JsonKeys.VERIFY)
    logger.debug("verify method starts with value {}", rootNode)
    val signReq: Map[String, AnyRef] = Map(JsonKeys.ENTITY -> rootNode)
    var result = false
    try {
      val response: HTTPResponse = httpUtil.post(verifyEndPoint, mapper.writeValueAsString(signReq))
      result = mapper.readValue(response.body, new TypeReference[Boolean]() {})
    } catch {
      case ex: ClientProtocolException =>
        logger.error("ClientProtocolException when verifying: {}", ex.getMessage)
        throw new SignatureException.UnreachableException(ex.getMessage)
      case e: Exception =>
        logger.error("Exception occurred  while verifying signature:{} ", e.getMessage)
        throw new SignatureException.VerificationException("")
    }
    result
  }

}
