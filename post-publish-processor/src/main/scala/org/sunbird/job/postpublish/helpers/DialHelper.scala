package org.sunbird.job.postpublish.helpers

import java.util

import com.google.gson.Gson
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.Metrics
import org.sunbird.job.models.ExtDataConfig
import org.sunbird.job.postpublish.domain.Event
import org.sunbird.job.task.PostPublishProcessorConfig
import org.sunbird.job.util.{CassandraUtil, HttpUtil, JSONUtil, Neo4JUtil}

import scala.collection.JavaConverters._

trait DialHelper {

  private[this] val logger = LoggerFactory.getLogger(classOf[DialHelper])
  val graphId = "domain"
  lazy private val gson = new Gson()


  def fetchExistingReservedDialcodes(edata: java.util.Map[String, AnyRef]): util.Map[String, Integer] = {
    val reservedDialcodes = edata.getOrDefault("reservedDialcodes", "{}").asInstanceOf[String]
    JSONUtil.deserialize[util.Map[String, Integer]](reservedDialcodes)
  }

  def reserveDialCodes(edata: java.util.Map[String, AnyRef], config: PostPublishProcessorConfig)(implicit httpUtil: HttpUtil): java.util.Map[String, Integer] = {
    val identifier = edata.get("identifier").asInstanceOf[String]
    val request = s"""{"request": { "dialcodes": {"count": 1, "qrCodeSpec": {"errorCorrectionLevel": "H"}}}}"""
    val headers = Map[String, String]("X-Channel-Id" -> edata.getOrDefault("channel", "").asInstanceOf[String],
      "Content-Type" -> "application/json")

    logger.info(s"Reserved Dialcode Api request body : ${request}")
    logger.info(s"Reserved Dialcode Api header : ${headers}")
    logger.info(s"Reserved Dialcode Api url : " + config.reserveDialCodeAPIPath + identifier)

    val response = httpUtil.post(config.reserveDialCodeAPIPath + "/" + identifier, request, headers)
    logger.info(s"Reserved Dialcode Api response status : ${response.status} and response body : ${response.body}")
    if (response.status == 200) {
      val responseBody = gson.fromJson(response.body, classOf[java.util.Map[String, AnyRef]])
      val reservedDialcodes: util.Map[String, Integer] = responseBody.getOrDefault("result", new util.HashMap[String, AnyRef])
        .asInstanceOf[util.Map[String, AnyRef]].getOrDefault("reservedDialcodes", new util.HashMap[String, Integer]())
        .asInstanceOf[util.Map[String, Integer]]
      reservedDialcodes
    } else throw new Exception(s"Couldn't reserve dialcodes for identifier: $identifier")
  }

  def updateDIALToObject(identifier: String, dialCode: String)(implicit neo4JUtil: Neo4JUtil) = {
    neo4JUtil.updateNodeProperty(identifier, "dialcodes", (s"""["${dialCode}"]"""))
    logger.info(s"Added Reserved Dialcode to node.")
  }

  def fetchExistingDialcodes(edata: java.util.Map[String, AnyRef]): util.List[String] = {
    edata.getOrDefault("dialcodes", new util.ArrayList()).asInstanceOf[util.List[String]]
  }

  def validatePrimaryCategory(edata: java.util.Map[String, AnyRef])(implicit config: PostPublishProcessorConfig) = {
    val primaryCategory = edata.get("primaryCategory").asInstanceOf[String]
    config.primaryCategories.contains(primaryCategory)
  }

  def validateQR(dialcode: String)(implicit extConfig: ExtDataConfig, cassandraUtil: CassandraUtil): Boolean = {
    getQRImageRecord(dialcode) match {
      case Some(url: String) => true
      case _ => false
    }
  }

  def getQRImageRecord(dialcode: String)(implicit extConfig: ExtDataConfig, cassandraUtil: CassandraUtil): Option[String] = {
    if (dialcode.isEmpty) throw new Exception("Invalid dialcode to read")
    val fileName = s"0_$dialcode"
    val query = s"select url from ${extConfig.keyspace}.${extConfig.table} where filename = '$fileName';"
      val result = cassandraUtil.findOne(query)
      if (result == null) None else Some(result.getString("url"))
  }

  def updateDialcodeRecord(dialcode: String, channel: String, ets: Long)(implicit extConfig: ExtDataConfig, cassandraUtil: CassandraUtil): Boolean = {
    if (dialcode.isEmpty) throw new Exception("Invalid dialcode to update")
    val query: String = s"insert into ${extConfig.keyspace}.${extConfig.table} (filename,channel,created_on,dialcode,status) values ('0_${dialcode}', '${channel}', ${ets}, '${dialcode}', 0);"
    if (cassandraUtil.upsert(query)) {
      logger.info(s"Added Dialcode to the table.")
      true
    } else {
      logger.error("There was an issue while inserting the dialcode details into table")
      throw new Exception("There was an issue while inserting the dialcode details into table")
    }
  }

  def createQRGeneratorEvent(edata: java.util.Map[String, AnyRef], dialcode: String, context: ProcessFunction[java.util.Map[String, AnyRef], String]#Context, config: PostPublishProcessorConfig)(implicit metrics: Metrics, extConfig: ExtDataConfig, cassandraUtil: CassandraUtil): Unit = {
    logger.info("Generating event for QR Image Generation.")
    val ets = System.currentTimeMillis
    val identifier = edata.get("identifier").asInstanceOf[String]
    val channelId = edata.getOrDefault("channel", "").asInstanceOf[String]

    if (updateDialcodeRecord(dialcode, channelId, ets)) {
      val event = s"""{"eid":"BE_QR_IMAGE_GENERATOR", "objectId": "${identifier}", "dialcodes": [{"data": "${config.dialBaseUrl}${dialcode}", "text": "${dialcode}", "id": "0_${dialcode}"}], "storage": {"container": "dial", "path": "${channelId}/", "fileName": "${identifier}_${ets}"}, "config": {"errorCorrectionLevel": "H", "pixelsPerBlock": 2, "qrCodeMargin": 3, "textFontName": "Verdana", "textFontSize": 11, "textCharacterSpacing": 0.1, "imageFormat": "png", "colourModel": "Grayscale", "imageBorderSize": 1}}""".stripMargin
      logger.info(s"QR Image Generator Event Object : ${event}")
      context.output(config.generateQRImageOutTag, event)
      metrics.incCounter(config.qrImageGeneratorEventCount)
    }

  }

  def getDialCodeDetails(identifier: String, event: Event)(implicit neo4JUtil: Neo4JUtil, config: PostPublishProcessorConfig): util.Map[String, AnyRef] = {
    logger.info("Process Dialcode Link for content: " + identifier)
    val metadata = neo4JUtil.getNodeProperties(identifier)

    if (validatePrimaryCategory(metadata)(config)) {
      logger.info(s"Primary Category match found. Starting the process for Dial Code Generation.")
      Map[String, AnyRef]("identifier" -> metadata.get("identifier"),
        "primaryCategory" -> metadata.getOrDefault("primaryCategory", ""),
        "contentType" -> metadata.getOrDefault("contentType", ""),
        "channel" -> metadata.getOrDefault("channel", ""),
        "dialcodes" -> metadata.getOrDefault("dialcodes", new util.ArrayList[String] {}),
        "reservedDialcodes" -> metadata.getOrDefault("reservedDialcodes", "{}")).asJava
    } else {
      logger.info(s"Primary Category does not match. Skipping the process for Dial Code Generation.")
      new util.HashMap[String, AnyRef]()
    }
  }
}