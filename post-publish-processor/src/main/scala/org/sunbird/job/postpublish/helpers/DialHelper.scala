package org.sunbird.job.postpublish.helpers

import java.util

import com.google.gson.Gson
import org.slf4j.LoggerFactory
import org.sunbird.job.functions.DIALCodeLinkFunction
import org.sunbird.job.task.PostPublishProcessorConfig
import org.sunbird.job.util.{HttpUtil, JSONUtil, Neo4JUtil}

import scala.collection.JavaConverters._

trait DialHelper {

    private[this] val logger = LoggerFactory.getLogger(classOf[DIALCodeLinkFunction])
    val graphId = "domain"
    lazy private val gson = new Gson()


    def fetchExistingReservedDialcodes(edata: java.util.Map[String, AnyRef]): util.Map[String, Integer] = {
        val reservedDialcodes = edata.getOrDefault("reservedDialcodes", "{}").asInstanceOf[String]
        JSONUtil.deserialize[util.Map[String, Integer]](reservedDialcodes)
    }

    def reserveDialCodes(edata: java.util.Map[String, AnyRef])(implicit httpUtil: HttpUtil): java.util.Map[String, Integer] = {
        val identifier = edata.get("identifier").asInstanceOf[String]
        val request = Map("request" -> Map("dialcodes" -> Map("count" -> 1.asInstanceOf[AnyRef],
            "qrCodeSpec" -> Map("errorCorrectionLevel" -> "H").asJava).asJava).asJava).asJava
        val headers: util.Map[String, String] = Map[String, String]("X-Channel-Id" -> edata.getOrDefault("channel", "").asInstanceOf[String],
            "Content-Type" -> "application/json").asJava
        logger.info(s"Reserved Dialcode Api request body : ${request}")
        val httpRequest = JSONUtil.serialize(request)
        val response = httpUtil.post(s"https://dev.sunbirded.org/action/content/v3/dialcode/reserve/$identifier", httpRequest, headers)
        if (response.status == 200) {
            val responseBody = gson.fromJson(response.body, classOf[java.util.Map[String, AnyRef]])
            logger.info(s"Reserved Dialcode Api resopnse body : ${responseBody}")
            val reservedDialcodes = responseBody.getOrDefault("result", new util.HashMap[String, AnyRef])
                .asInstanceOf[util.Map[String, AnyRef]].getOrDefault("reservedDialcodes", new util.HashMap[String, Integer]())
                .asInstanceOf[util.Map[String, Integer]]
            reservedDialcodes
        } else throw new Exception(s"Couldn't reserve dialcodes for identifier: $identifier")
    }


    def updateDIALToObject(identifier: String, dialCode: String)(implicit neo4JUtil: Neo4JUtil) = {
        neo4JUtil.updateNodeProperty(identifier, "dialcodes", dialCode)
        logger.info(s"Added Reserved Dialcode to node.")
    }

    def fetchExistingDialcodes(edata: java.util.Map[String, AnyRef]): util.List[String] = {
        edata.getOrDefault("dialcodes", "[]").asInstanceOf[util.List[String]]
    }

     def validateContentType(edata: java.util.Map[String, AnyRef])(implicit config: PostPublishProcessorConfig) = {
        val contentType = edata.get("contentType").asInstanceOf[String]
        config.contentTypes.contains(contentType)
    }
}
