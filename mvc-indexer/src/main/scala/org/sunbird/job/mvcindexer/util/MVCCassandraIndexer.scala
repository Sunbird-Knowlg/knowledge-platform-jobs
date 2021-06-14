package org.sunbird.job.mvcindexer.util

import com.datastax.driver.core.BoundStatement
import com.datastax.driver.core.querybuilder.Update.Assignments
import com.datastax.driver.core.querybuilder.{QueryBuilder, Update}
import org.apache.commons.lang3.StringUtils
import org.sunbird.job.mvcindexer.domain.Event
import org.sunbird.job.util.{CassandraUtil, HTTPResponse, HttpUtil, JSONUtil}
//import org.json.JSONArray
//import org.json.JSONObject
import org.slf4j.LoggerFactory
import org.sunbird.job.mvcindexer.service.MVCIndexerService
import org.sunbird.job.mvcindexer.task.MVCIndexerConfig
import scala.collection.mutable.{Map => MutableMap}

class MVCCassandraIndexer(config: MVCIndexerConfig, cassandraUtil: CassandraUtil, httpUtil: HttpUtil) {
  val mlworkbenchapirequest = "{\"request\":{ \"input\" :{ \"content\" : [] } } }"
  val mlvectorListRequest = "{\"request\":{\"text\":[],\"cid\": \"\",\"language\":\"en\",\"method\":\"BERT\",\"params\":{\"dim\":768,\"seq_len\":25}}}"
  jobname = "vidyadaan_content_keyword_tagging"
  private[util] var jobname = ""
  val mapStage1:MutableMap[String, AnyRef] = MutableMap[String, AnyRef]()
  private[this] lazy val logger = LoggerFactory.getLogger(classOf[MVCCassandraIndexer])

  // Insert to cassandra
  @throws[Exception]
  def insertIntoCassandra(message: Event, identifier: String): Unit = {
    val obj: Map[String, AnyRef] = message.eventData
    val action = message.action
    if (StringUtils.isNotBlank(action)) if (action.equalsIgnoreCase("update-es-index")) {
      logger.info("getContentMetaData ::: extracting required fields" + obj)
      extractFieldsToBeInserted(obj)
      logger.info("getContentMetaData ::: making ml workbench api request")
      getMLKeywords(obj)
      logger.info("insertIntoCassandra ::: update-es-index-1 event")
      logger.info("insertIntoCassandra ::: Inserting into cassandra stage-1")
      updateContentProperties(identifier, mapStage1)
    }
    else if (action.equalsIgnoreCase("update-ml-keywords")) {
      logger.info("insertIntoCassandra ::: update-ml-keywords")

      getMLVectors(message.mlContentText, identifier)
      val mapForStage2 = MutableMap[String, AnyRef]()
      mapForStage2 += ("ml_keywords" -> message.mlKeywords)
      mapForStage2 += ("ml_content_text"-> message.mlContentText)
      updateContentProperties(identifier, mapForStage2)
    }
    else if (action.equalsIgnoreCase("update-ml-contenttextvector")) {
      logger.info("insertIntoCassandra ::: update-ml-contenttextvector event")
      val mapForStage3 = MutableMap[String, AnyRef]()
      mapForStage3 += ("ml_content_text_vector"-> message.mlContentTextVector)
      updateContentProperties(identifier, mapForStage3)
    }
  }

  //Getting Fields to be inserted into cassandra
  private def extractFieldsToBeInserted(contentobj: Map[String, AnyRef]): Unit = {
    val fields = Map[String, String]("level1Concept"-> "level1_concept", "level2Concept"-> "level2_concept",
      "level3Concept"-> "level3_concept", "textbook_name"-> "textbook_name", "level1Name"-> "level1_name",
      "level2Name"->  "level2_name", "level3Name"-> "level3_name")
    for ((fieldKey: String,fieldValue: String) <- fields) {
      if (contentobj.contains(fieldKey)) {
        mapStage1.put(fieldValue, contentobj(fieldKey).asInstanceOf[List[String]])
      }
    }

    if (contentobj.contains("source")) mapStage1.put("source", contentobj.get("source"))
    if (contentobj.contains("sourceURL")) mapStage1.put("sourceurl", contentobj.get("sourceURL"))
    logger.info("extractedmetadata")
  }

  // POST reqeuest for ml keywords api
  @throws[Exception]
  private[util] def getMLKeywords(contentdef: Map[String, AnyRef]): Unit = {
    val obj = JSONUtil.deserialize[Map[String, AnyRef]](mlworkbenchapirequest)
    val req = obj("request").asInstanceOf[Map[String, AnyRef]]
    val input = req("input").asInstanceOf[Map[String, AnyRef]]
    var content = input("content").asInstanceOf[List[Map[String, AnyRef]]]
    content :+= contentdef
//    req.put("job", jobname)

    val requestBody = JSONUtil.serialize(obj)
    logger.info("getMLKeywords ::: The ML workbench URL is " + "http://" + config.mlKeywordAPI + ":3579/daggit/submit")
    try {
      val resp:HTTPResponse = httpUtil.post("http://" + config.mlKeywordAPI + ":3579/daggit/submit", requestBody)
      logger.info("getMLKeywords ::: The ML workbench response is " + resp)
    } catch {
      case e: Exception =>
        logger.info("getMLKeywords ::: ML workbench api request failed ")
    }
  }

  // Post reqeuest for vector api
  @throws[Exception]
  def getMLVectors(contentText: String, identifier: String): Unit = {
    val obj = JSONUtil.deserialize[MutableMap[String, AnyRef]](mlvectorListRequest)
    val req = obj.get("request").asInstanceOf[MutableMap[String, AnyRef]]
    var text = req.get("text").asInstanceOf[List[String]]
    req.put("cid", identifier)
    text :+= contentText
    val requestBody = JSONUtil.serialize(obj)
    logger.info("getMLVectors ::: The ML vector URL is " + "http://" + config.mlVectorAPI + ":1729/ml/vector/ContentText")
    try {
      val resp:HTTPResponse = httpUtil.post("http://" + config.mlVectorAPI + ":1729/ml/vector/ContentText", requestBody)
      logger.info("getMLVectors ::: ML vector api request response is " + resp)
    } catch {
      case e: Exception =>
        logger.info("getMLVectors ::: ML vector api request failed ")
    }
  }

  def updateContentProperties(contentId: String, map: MutableMap[String, AnyRef]): Unit = {
    if (null == map || map.isEmpty) return

    try {
      val query:Update = QueryBuilder.update(config.dbKeyspace, config.dbTable)
      var queryAssignments:Assignments = null
      var i = 0
      for ((key, value:Some[AnyRef]) <- map.toList) {
        if (null != value && null != key) {
          if (i==0) {
            queryAssignments = query.`with`(QueryBuilder.set(key, value.get))
          } else {
            queryAssignments = queryAssignments.and(QueryBuilder.set(key, value.get))
          }
        } else {
          return
        }
        i += 1
      }

      queryAssignments.and(QueryBuilder.set("last_updated_on", "dateOf(now())"))
      val finalQuery = queryAssignments.where(QueryBuilder.eq("content_id", contentId))
      logger.info("Executing the statement to insert into cassandra for identifier  " + contentId)
      cassandraUtil.session.execute(finalQuery)
    } catch {
      case e: Exception =>
        logger.error("Exception while inserting data into cassandra for " + contentId, e)
        throw e
    }
  }
}
