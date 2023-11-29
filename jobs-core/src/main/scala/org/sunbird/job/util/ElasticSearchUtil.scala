package org.sunbird.job.util

import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.commons.lang3.StringUtils
import org.apache.http.HttpHost
import org.apache.http.client.config.RequestConfig
import org.elasticsearch.action.admin.indices.alias.Alias
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.delete.DeleteRequest
import org.elasticsearch.action.get.GetRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.update.UpdateRequest
import org.elasticsearch.client.indices.GetIndexRequest
import org.elasticsearch.client.{RequestOptions, RestClient, RestHighLevelClient}
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.xcontent.XContentType
import org.slf4j.LoggerFactory

import java.io.IOException
import java.util

class ElasticSearchUtil(connectionInfo: String, indexName: String, indexType: String, batchSize: Int = 1000) extends Serializable {

  private val resultLimit = 100
  private val esClient: RestHighLevelClient = createClient(connectionInfo)
  private val mapper = new ObjectMapper
  private val maxFieldLimit = 32000

  private[this] val logger = LoggerFactory.getLogger(classOf[ElasticSearchUtil])

  System.setProperty("es.set.netty.runtime.available.processors", "false")

  private def createClient(connectionInfo: String): RestHighLevelClient = {
    val httpHosts: Array[HttpHost] = connectionInfo.split(",").map(info => {
      val host = info.split(":")(0)
      val port = info.split(":")(1).toInt
      new HttpHost(host, port)
    })

    val restClientBuilder = RestClient.builder(httpHosts: _*)
      .setRequestConfigCallback((requestConfigBuilder: RequestConfig.Builder) => {
        requestConfigBuilder.setConnectionRequestTimeout(-1)
      })

    new RestHighLevelClient(restClientBuilder)
  }


  def isIndexExists(): Boolean = {
    try {
      val response = esClient.indices().get(new GetIndexRequest(indexName), RequestOptions.DEFAULT)
      response.getIndices.contains(indexName)
    } catch {
      case e: IOException =>
        logger.error(s"ElasticSearchUtil:: Failed to check Index if Present or not. Exception: ", e)
        false
    }
  }


  def addIndex(settings: String, mappings: String, alias: String = ""): Boolean = {
    var response = false
    val client = esClient
    if (!isIndexExists()) {
      val createRequest = new CreateIndexRequest(indexName)
      if (StringUtils.isNotBlank(alias)) createRequest.alias(new Alias(alias))
      if (StringUtils.isNotBlank(settings)) createRequest.settings(Settings.builder.loadFromSource(settings, XContentType.JSON))
      if (StringUtils.isNotBlank(indexType) && StringUtils.isNotBlank(mappings)) createRequest.mapping(indexType, mappings, XContentType.JSON)
      try {
        val createIndexResponse = client.indices().create(createRequest, RequestOptions.DEFAULT)
        response = createIndexResponse.isAcknowledged
      } catch {
        case e: IOException =>
          logger.error(s"ElasticSearchUtil:: Error while adding index. Exception: ", e)
      }
    }
    response
  }

  def addDocument(identifier: String, document: String): Unit = {
    try {
      val doc = mapper.readValue(document, new TypeReference[util.Map[String, AnyRef]]() {})
      val updatedDoc = checkDocStringLength(doc)
      val indexRequest = new IndexRequest(indexName, indexType, identifier).source(updatedDoc)
      val response = esClient.index(indexRequest, RequestOptions.DEFAULT)
      logger.info(s"Added ${response.getId} to index ${response.getIndex}")
    } catch {
      case e: IOException =>
        logger.error(s"ElasticSearchUtil:: Error while adding document to index: $indexName", e)
    }
  }

  @throws[IOException]
  def addDocumentWithIndex(document: String, indexName: String, identifier: String = null): Unit = {
    try {
      val doc = mapper.readValue(document, new TypeReference[util.Map[String, AnyRef]]() {})
      val updatedDoc = checkDocStringLength(doc)
      val indexRequest = if (identifier == null) new IndexRequest(indexName, indexType) else new IndexRequest(indexName, indexType, identifier)
      val response = esClient.index(indexRequest.source(updatedDoc), RequestOptions.DEFAULT)
      logger.info(s"Added ${response.getId} to index ${response.getIndex}")
    } catch {
      case e: IOException =>
        logger.error(s"ElasticSearchUtil:: Error while adding document to index: $indexName : " + e.getMessage)
        e.printStackTrace()
        throw e
    }
  }

  def updateDocument(identifier: String, document: String): Unit = {
    try {
      val doc = mapper.readValue(document, new TypeReference[util.Map[String, AnyRef]]() {})
      val updatedDoc = checkDocStringLength(doc)
      val indexRequest = new IndexRequest(indexName, indexType, identifier).source(updatedDoc)
      val request = new UpdateRequest(indexName, indexType, identifier).doc(updatedDoc).upsert(indexRequest)
      val response = esClient.update(request, RequestOptions.DEFAULT)
      logger.info(s"Updated ${response.getId} to index ${response.getIndex}")
    } catch {
      case e: IOException =>
        logger.error(s"ElasticSearchUtil:: Error while updating document to index: $indexName", e)
    }
  }

  def deleteDocument(identifier: String): Unit = {
    val deleteRequest = new DeleteRequest(indexName, indexType, identifier)
    val response = esClient.delete(deleteRequest, RequestOptions.DEFAULT)
    logger.info(s"Deleted ${response.getId} to index ${response.getIndex}")
  }

  def getDocumentAsString(identifier: String): String = {
    val getRequest = new GetRequest(indexName, indexType, identifier)
    val response = esClient.get(getRequest, RequestOptions.DEFAULT)
    response.getSourceAsString
  }

  def close(): Unit = {
    if (esClient != null) {
      try {
        esClient.close()
      } catch {
        case e: IOException => e.printStackTrace()
      }
    }
  }

  @throws[Exception]
  def bulkIndexWithIndexId(indexName: String, documentType: String, jsonObjects: Map[String, AnyRef]): Unit = {
    if (isIndexExists(indexName)) {
      if (jsonObjects.nonEmpty) {
        var count = 0
        val request = new BulkRequest
        for (key <- jsonObjects.keySet) {
          count += 1
          val document = ScalaJsonUtil.serialize(jsonObjects(key).asInstanceOf[Map[String, AnyRef]])
          logger.debug("ElasticSearchUtil:: bulkIndexWithIndexId:: document: " + document)
          val doc: util.Map[String, AnyRef] = mapper.readValue(document, new TypeReference[util.Map[String, AnyRef]]() {})
          val updatedDoc = checkDocStringLength(doc)
          logger.debug("ElasticSearchUtil:: bulkIndexWithIndexId:: doc: " + updatedDoc)
          request.add(new IndexRequest(indexName, documentType, key).source(updatedDoc))
          if (count % batchSize == 0 || (count % batchSize < batchSize && count == jsonObjects.size)) {
            try {
              val bulkResponse = esClient.bulk(request, RequestOptions.DEFAULT)
              if (bulkResponse.hasFailures) logger.info("ElasticSearchUtil:: bulkIndexWithIndexId:: Failures in Elasticsearch bulkIndex : " + bulkResponse.buildFailureMessage)
            } catch {
              case e: IOException =>
                logger.error(s"ElasticSearchUtil:: Error while performing bulk index operation. Exception: ", e)
            }
          }
        }
      }
    } else {
      throw new Exception("ElasticSearchUtil:: Index does not exist: " + indexName)
    }
  }

  def isIndexExists(indexName: String): Boolean = {
    try {
      val request = new GetIndexRequest(indexName)
      val response = esClient.indices().exists(request, RequestOptions.DEFAULT)
      response
    } catch {
      case e: IOException =>
        false
    }
  }


  private def checkDocStringLength(doc: util.Map[String, AnyRef]): util.Map[String, AnyRef] = {
    doc.entrySet().forEach(entry => {
      if (entry.getValue.isInstanceOf[String] && entry.getValue.toString.length > maxFieldLimit) {
        doc.put(entry.getKey, entry.getValue.toString.substring(0, maxFieldLimit))
      }
    })
    doc
  }
}