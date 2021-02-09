package org.sunbird.job.postpublish.helpers

import java.lang.reflect.Type
import java.util

import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import org.slf4j.LoggerFactory
import org.sunbird.job.functions.DIALCodeLinkFunction
import org.sunbird.job.util.{HttpUtil, JSONUtil, Neo4JUtil}

import scala.collection.JavaConverters._

trait DialUtility {

    private[this] val logger = LoggerFactory.getLogger(classOf[DIALCodeLinkFunction])
    val mapType: Type = new TypeToken[java.util.Map[String, AnyRef]]() {}.getType
    val graphId = "domain"
    lazy private val gson = new Gson()


    def fetchExistingReservedDialcodes(event: java.util.Map[String, AnyRef])(implicit neo4JUtil: Neo4JUtil): util.Map[String, Integer] = {
        val identifier = event.getOrDefault("identifier", "").asInstanceOf[String]
        if (identifier.isEmpty) throw new Exception("Identifier is a mandatory property")
        val metadata = neo4JUtil.getNodeProperties(identifier)
        if (metadata.isEmpty) throw new Exception("No such content found")
        JSONUtil.deserialize[util.Map[String, Integer]](metadata.getOrDefault("reservedDialcodes", "{}").asInstanceOf[String])
    }

    def reserveDialCodes(metadata: java.util.Map[String, AnyRef])(implicit httpUtil: HttpUtil): java.util.Map[String, Integer] = {
        val identifier = metadata.getOrDefault("identifier", "")
        val request = Map("request" -> Map("dialcodes" -> Map("count" -> 1.asInstanceOf[AnyRef],
            "qrCodeSpec" -> Map("errorCorrectionLevel" -> "H").asJava).asJava).asJava).asJava
        val headers: util.Map[String, String] = Map[String, String]("X-Channel-Id" -> metadata.getOrDefault("channel", "").asInstanceOf[String],
            "Content-Type" -> "application/json").asJava
        val httpRequest = JSONUtil.serialize(request)
        val response = httpUtil.post(s"https://dev.sunbirded.org/action/content/v3/dialcode/reserve/$identifier", httpRequest, headers)
        if (response.status == 200) {
            val responseBody = gson.fromJson(response.body, classOf[java.util.Map[String, AnyRef]])
            val reservedDialcodes = responseBody.getOrDefault("result", new util.HashMap[String, AnyRef])
                .asInstanceOf[util.Map[String, AnyRef]].getOrDefault("reservedDialcodes", new util.HashMap[String, Integer]())
                .asInstanceOf[util.Map[String, Integer]]
            reservedDialcodes
        } else throw new Exception(s"Couldn't reserve dialcodes for identifier: $identifier")
    }


    def updateDIALToObject(identifier: String, dialCode: String)(implicit neo4JUtil: Neo4JUtil) = {
        neo4JUtil.updateNodeProperty(identifier, "dialcodes", dialCode)
    }

    def fetchExistingDialcodes(event: java.util.Map[String, AnyRef])(implicit neo4JUtil: Neo4JUtil): util.List[String] = {
        val identifier = event.getOrDefault("identifier", "").asInstanceOf[String]
        if (identifier.isEmpty) throw new Exception("Identifier is a mandatory property")
        val metadata = neo4JUtil.getNodeProperties(identifier)
        if (metadata.isEmpty) throw new Exception("No such content found")
        JSONUtil.deserialize[util.List[String]](metadata.getOrDefault("dialcodes", "[]").asInstanceOf[String])
    }
}
