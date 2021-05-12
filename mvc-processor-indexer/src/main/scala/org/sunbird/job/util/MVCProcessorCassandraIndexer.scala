package org.sunbird.job.util

import org.apache.commons.lang3.StringUtils
import org.json.JSONArray
import org.json.JSONObject
import org.slf4j.LoggerFactory
import org.sunbird.job.service.MVCProcessorIndexerService
import org.sunbird.job.task.MVCProcessorIndexerConfig

class MVCProcessorCassandraIndexer(config: MVCProcessorIndexerConfig) {
  val mlworkbenchapirequest = "{\"request\":{ \"input\" :{ \"content\" : [] } } }"
  val mlvectorListRequest = "{\"request\":{\"text\":[],\"cid\": \"\",\"language\":\"en\",\"method\":\"BERT\",\"params\":{\"dim\":768,\"seq_len\":25}}}"
  jobname = "vidyadaan_content_keyword_tagging"
  private[util] var jobname = ""
  private[util] val mapStage1 = new Nothing
  private[util] var level1concept = null
  private[util] var level2concept = null
  private[util] var level3concept = null
  private[util] var textbook_name = null
  private[util] var level1_name = null
  private[util] var level2_name = null
  private[util] var level3_name = null
  private[this] lazy val logger = LoggerFactory.getLogger(classOf[MVCProcessorIndexerService])

  // Insert to cassandra
  @throws[Exception]
  def insertIntoCassandra(obj: Map[String, AnyRef], identifier: String): Unit = {
    val action = obj.get("action").toString
    if (StringUtils.isNotBlank(action)) if (action.equalsIgnoreCase("update-es-index")) {
      logger.info("MVCProcessorCassandraIndexer :: getContentMetaData ::: extracting required fields" + obj)
      extractFieldsToBeInserted(obj)
      logger.info("MVCProcessorCassandraIndexer :: getContentMetaData ::: making ml workbench api request")
      getMLKeywords(obj)
      logger.info("MVCProcessorCassandraIndexer :: insertIntoCassandra ::: update-es-index-1 event")
      logger.info("MVCProcessorCassandraIndexer :: insertIntoCassandra ::: Inserting into cassandra stage-1")
      CassandraConnector.updateContentProperties(identifier, mapStage1)
    }
    else if (action.equalsIgnoreCase("update-ml-keywords")) {
      logger.info("MVCProcessorCassandraIndexer :: insertIntoCassandra ::: update-ml-keywords")
      var ml_contentText = null
      var ml_Keywords = null
      ml_contentText = if (obj.get("ml_contentText") != null) obj.get("ml_contentText").toString
      else null
      ml_Keywords = if (obj.get("ml_Keywords") != null) obj.get("ml_Keywords").asInstanceOf[Nothing]
      else null
      getMLVectors(ml_contentText, identifier)
      val mapForStage2 = new Nothing
      mapForStage2.put("ml_keywords", ml_Keywords)
      mapForStage2.put("ml_content_text", ml_contentText)
      CassandraConnector.updateContentProperties(identifier, mapForStage2)
    }
    else if (action.equalsIgnoreCase("update-ml-contenttextvector")) {
      logger.info("MVCProcessorCassandraIndexer :: insertIntoCassandra ::: update-ml-contenttextvector event")
      var ml_contentTextVectorList = null
      var ml_contentTextVector = null
      ml_contentTextVectorList = if (obj.get("ml_contentTextVector") != null) obj.get("ml_contentTextVector").asInstanceOf[Nothing]
      else null
      if (ml_contentTextVectorList != null) ml_contentTextVector = new Nothing(ml_contentTextVectorList.get(0))
      val mapForStage3 = new Nothing
      mapForStage3.put("ml_content_text_vector", ml_contentTextVector)
      CassandraConnector.updateContentProperties(identifier, mapForStage3)
    }
  }

  //Getting Fields to be inserted into cassandra
  private def extractFieldsToBeInserted(contentobj: Map[String, AnyRef]): Unit = {
    val fields = Map[String, String]("level1Concept"-> "level1_concept", "level2Concept"-> "level2_concept",
      "level3Concept"-> "level3_concept", "textbook_name"-> "textbook_name", "level1Name"-> "level1_name",
      "level2Name"->  "level2_name", "level3Name"-> "level3_name")
    for ((fieldKey: String,fieldValue: String) <- fields) {
      if (contentobj.contains(fieldKey)) {
        mapStage1.put(fieldValue, contentobj.get(fieldKey).asInstanceOf[List[String]])
      }
    }
//    if (contentobj.contains("level1Concept")) {
//      level1concept = contentobj.get("level1Concept").asInstanceOf[List[String]]
//      mapStage1.put("level1_concept", level1concept)
//    }
//    if (contentobj.contains("level2Concept")) {
//      level2concept = contentobj.get("level2Concept").asInstanceOf[List[String]]
//      mapStage1.put("level2_concept", level2concept)
//    }
//    if (contentobj.contains("level3Concept")) {
//      level3concept = contentobj.get("level3Concept").asInstanceOf[Nothing]
//      mapStage1.put("level3_concept", level3concept)
//    }
//    if (contentobj.contains("textbook_name")) {
//      textbook_name = contentobj.get("textbook_name").asInstanceOf[Nothing]
//      mapStage1.put("textbook_name", textbook_name)
//    }
//    if (contentobj.contains("level1Name")) {
//      level1_name = contentobj.get("level1Name").asInstanceOf[Nothing]
//      mapStage1.put("level1_name", level1_name)
//    }
//    if (contentobj.contains("level2Name")) {
//      level2_name = contentobj.get("level2Name").asInstanceOf[Nothing]
//      mapStage1.put("level2_name", level2_name)
//    }
//    if (contentobj.contains("level3Name")) {
//      level3_name = contentobj.get("level3Name").asInstanceOf[Nothing]
//      mapStage1.put("level3_name", level3_name)
//    }
    if (contentobj.contains("source")) mapStage1.put("source", contentobj.get("source"))
    if (contentobj.contains("sourceURL")) mapStage1.put("sourceurl", contentobj.get("sourceURL"))
    logger.info("MVCProcessorCassandraIndexer :: extractedmetadata")
  }

  // POST reqeuest for ml keywords api
  @throws[Exception]
  private[util] def getMLKeywords(contentdef: Map[String, AnyRef]): Unit = {
    val obj = new Nothing(mlworkbenchapirequest)
    val req = (obj.get("request")).asInstanceOf[Nothing]
    val input = req.get("input").asInstanceOf[Nothing]
    val content = input.get("content").asInstanceOf[Nothing]
    content.put(contentdef)
    req.put("job", jobname)
    logger.info("MVCProcessorCassandraIndexer :: getMLKeywords  ::: The ML workbench URL is " + "http://" + config.mlKeywordAPI + ":3579/daggit/submit")
    try {
      val resp = HTTPUtil.makePostRequest("http://" + config.mlKeywordAPI + ":3579/daggit/submit", obj.toString)
      logger.info("MVCProcessorCassandraIndexer :: getMLKeywords  ::: The ML workbench response is " + resp)
    } catch {
      case e: Nothing =>
        logger.info("MVCProcessorCassandraIndexer :: getMLKeywords  ::: ML workbench api request failed ")
    }
  }

  // Post reqeuest for vector api
  @throws[Exception]
  def getMLVectors(contentText: String, identifier: String): Unit = {
    val obj = new Nothing(mlvectorListRequest)
    val req = (obj.get("request")).asInstanceOf[Nothing]
    val text = req.get("text").asInstanceOf[Nothing]
    req.put("cid", identifier)
    text.put(contentText)
    logger.info("MVCProcessorCassandraIndexer :: getMLVectors  ::: The ML vector URL is " + "http://" + config.mlVectorAPI + ":1729/ml/vector/ContentText")
    try {
      val resp = HttpUtil.post("http://" + config.mlVectorAPI + ":1729/ml/vector/ContentText", obj.toString)
      logger.info("MVCProcessorCassandraIndexer :: getMLVectors  ::: ML vector api request response is " + resp)
    } catch {
      case e: Exception =>
        logger.info("MVCProcessorCassandraIndexer :: getMLVectors  ::: ML vector api request failed ")
    }
  }
}
