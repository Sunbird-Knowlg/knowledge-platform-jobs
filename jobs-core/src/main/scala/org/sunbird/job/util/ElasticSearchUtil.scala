package org.sunbird.job.util

import java.io.IOException
import java.util
import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.commons.lang3.StringUtils
import org.apache.http.HttpHost
import org.apache.http.client.config.RequestConfig
import org.apache.http.util.EntityUtils
import org.elasticsearch.action.DocWriteResponse
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest
import org.elasticsearch.action.bulk.{BulkItemResponse, BulkRequest}
import org.elasticsearch.action.delete.DeleteRequest
import org.elasticsearch.action.get.{GetRequest, MultiGetRequest}
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.search.{SearchRequest, SearchResponse}
import org.elasticsearch.action.update.UpdateRequest
import org.elasticsearch.client.{RestClient, RestClientBuilder, RestHighLevelClient}
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.index.query.{QueryBuilder, QueryBuilders}
import org.elasticsearch.search.SearchHits
import org.elasticsearch.search.aggregations.bucket.terms.Terms
import org.elasticsearch.search.aggregations.{AggregationBuilders, Aggregations}
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.slf4j.LoggerFactory
import scala.collection.JavaConverters._

class ElasticSearchUtil(indexName: String, indexType: String, connectionInfo: String, batchSize: Int = 1000) {

  private val resultLimit = 100
  private val esClient: RestHighLevelClient = createClient(connectionInfo)
  private val mapper = new ObjectMapper

  private[this] val logger = LoggerFactory.getLogger(classOf[ElasticSearchUtil])

  System.setProperty("es.set.netty.runtime.available.processors", "false")

  private def createClient(connectionInfo: String): RestHighLevelClient = {
    val httpHosts: List[HttpHost] = connectionInfo.split(",").map(info => {
      val host = info.split(":")(0)
      val port = info.split(":")(1).toInt
      new HttpHost(host, port)
    }).toList

    val builder: RestClientBuilder = RestClient.builder(httpHosts: _*).setRequestConfigCallback(new RestClientBuilder.RequestConfigCallback() {
      override def customizeRequestConfig(requestConfigBuilder: RequestConfig.Builder): RequestConfig.Builder = {
        requestConfigBuilder.setConnectionRequestTimeout(-1)
      }
    })
    new RestHighLevelClient(builder)
  }

  def isIndexExists(): Boolean = {
    try {
      val response = esClient.getLowLevelClient.performRequest("HEAD", "/" + indexName)
      response.getStatusLine.getStatusCode == 200
    } catch {
      case e: IOException => {
        logger.error("Failed to check Index if Present or not. Exception : ", e)
        false
      }
    }
  }

  def addIndex(settings: String, mappings: String): Boolean = {
    var response = false
    val client = esClient
    if (!isIndexExists()) {
      val createRequest = new CreateIndexRequest(indexName)
      if (StringUtils.isNotBlank(settings)) createRequest.settings(Settings.builder.loadFromSource(settings, XContentType.JSON))
      if (StringUtils.isNotBlank(indexType) && StringUtils.isNotBlank(mappings)) createRequest.mapping(indexType, mappings, XContentType.JSON)
      val createIndexResponse = client.indices.create(createRequest)
      response = createIndexResponse.isAcknowledged
    }
    response
  }

  def addDocumentWithId(documentId: String, document: String): Unit = {
    try {
      val doc = mapper.readValue(document, new TypeReference[util.Map[String, AnyRef]]() {})
      val response = esClient.index(new IndexRequest(indexName, indexType, documentId).source(doc))
      logger.info(s"Added ${response.getId} to index ${response.getIndex}")
    } catch {
      case e: IOException =>
        logger.error(s"Error while adding document to index : $indexName", e)
    }
  }

  def addDocument(document: String): Unit = {
    try {
      val doc = mapper.readValue(document, new TypeReference[util.Map[String, AnyRef]]() {})
      val response = esClient.index(new IndexRequest(indexName, indexType).source(doc))
      logger.info(s"Added ${response.getId} to index ${response.getIndex}")
    } catch {
      case e: IOException =>
        logger.error(s"Error while adding document to index : $indexName", e)
    }
  }

  def updateDocument(document: String, documentId: String): Unit = {
    try {
      val doc = mapper.readValue(document, new TypeReference[util.Map[String, AnyRef]]() {})
      val indexRequest = new IndexRequest(indexName, indexType, documentId).source(doc)
      val request = new UpdateRequest().index(indexName).`type`(indexType).id(documentId).doc(doc).upsert(indexRequest)
      val response = esClient.update(request)
      logger.info(s"Updated ${response.getId} to index ${response.getIndex}")
    } catch {
      case e: IOException =>
        logger.error(s"Error while updating document to index : $indexName", e)
    }
  }

  def deleteDocument(documentId: String): Unit = {
    val response = esClient.delete(new DeleteRequest(indexName, indexType, documentId))
    logger.info(s"Deleted ${response.getId} to index ${response.getIndex}")
  }

  def deleteDocumentsByQuery(query: QueryBuilder): Unit = {
    val response = esClient.getLowLevelClient.performRequest("POST", indexName + "/_delete_by_query" + query)
    logger.info(s"Deleted Documents by Query ${EntityUtils.toString(response.getEntity)}")
  }

  def getDocumentAsStringById(documentId: String): String = {
    val response = esClient.get(new GetRequest(indexName, indexType, documentId))
    response.getSourceAsString
  }

  def getMultiDocumentAsStringByIdList(documentIdList: util.List[String]): List[String] = {
    val request = new MultiGetRequest
    documentIdList.forEach((docId: String) => request.add(indexName, indexType, docId))
    esClient.multiGet(request).asScala.toStream.map(itemResponse => itemResponse.getResponse.getSourceAsString).toList
  }

  def bulkIndexWithIndexId(jsonObjects: util.Map[String, AnyRef]): Unit = {
    if (isIndexExists()) {
      val client = esClient
      if (!jsonObjects.isEmpty) {
        var count = 0
        val request = new BulkRequest
        jsonObjects.keySet.forEach(key => {
          count += 1
          request.add(new IndexRequest(indexName, indexType, key).source(jsonObjects.get(key).asInstanceOf[util.Map[String, AnyRef]]))
          if (count % batchSize == 0 || (count % batchSize < batchSize && count == jsonObjects.size)) {
            val bulkResponse = client.bulk(request)
            if (bulkResponse.hasFailures) logger.info(s"Failures in Elasticsearch bulkIndex : ${bulkResponse.buildFailureMessage}")
          }
        })
      }
    }
    else throw new Exception("Index does not exist: " + indexName)
  }

  def bulkIndexWithAutoGenerateIndexId(jsonObjects: util.List[util.Map[String, AnyRef]]): Unit = {
    if (isIndexExists()) {
      val client = esClient
      if (!jsonObjects.isEmpty) {
        var count = 0
        val request = new BulkRequest
        jsonObjects.forEach(json => {
          count += 1
          request.add(new IndexRequest(indexName, indexType).source(json))
          if (count % batchSize == 0 || (count % batchSize < batchSize && count == jsonObjects.size)) {
            val bulkResponse = client.bulk(request)
            if (bulkResponse.hasFailures) logger.info(s"Failures in Elasticsearch bulkIndex : ${bulkResponse.buildFailureMessage}")
          }
        })
      }
    }
    else throw new Exception("Index does not exist")
  }

  def textSearch(objectClass: Class[AnyRef], matchCriterias: util.Map[String, AnyRef], limit: Int): List[AnyRef] = {
    val result = search(matchCriterias, null, null, false, limit)
    getDocumentsFromSearchResult(result, objectClass)
  }

  def getDocumentsFromSearchResult(result: SearchResponse, objectClass: Class[AnyRef]): List[AnyRef] = {
    getDocumentsFromHits(result.getHits)
  }

  def getDocumentsFromHits(hits: SearchHits): List[AnyRef] = {
    hits.getHits.map(hit => hit.getSourceAsMap).toList
  }

  def getDocumentsFromSearchResultWithScore(result: SearchResponse): List[util.Map[String, AnyRef]] = {
    getDocumentsFromHitsWithScore(result.getHits)
  }

  def getDocumentsFromHitsWithScore(hits: SearchHits): List[util.Map[String, AnyRef]] = {
    hits.getHits.map(hit => {
      hit.getSourceAsMap.put("score", hit.getScore.asInstanceOf[AnyRef]).asInstanceOf[util.Map[String, AnyRef]]
    }).toList
  }

  def textSearchReturningId(matchCriterias: util.Map[String, AnyRef]): List[util.Map[String, AnyRef]] = {
    val result = search(matchCriterias, null, null, false, 100)
    getDocumentsFromSearchResultWithId(result)
  }

  def getDocumentsFromSearchResultWithId(result: SearchResponse): List[util.Map[String, AnyRef]] = {
    getDocumentsFromHitsWithId(result.getHits)
  }

  def getDocumentsFromHitsWithId(hits: SearchHits): List[util.Map[String, AnyRef]] = {
    hits.getHits.map(hit => hit.getSourceAsMap.put("id", hit.getId).asInstanceOf[util.Map[String, AnyRef]])
      .toList
  }

  def wildCardSearch(objectClass: Class[AnyRef], textKeyWord: String, wordWildCard: String, limit: Int): List[AnyRef] = {
    val result = wildCardSearch(textKeyWord, wordWildCard, limit)
    getDocumentsFromSearchResult(result, objectClass)
  }

  def wildCardSearch(textKeyWord: String, wordWildCard: String, limit: Int): SearchResponse = {
    val query = buildJsonForWildCardQuery(textKeyWord, wordWildCard)
    query.size(limit)
    search(query)
  }

  def textFiltersSearch(objectClass: Class[AnyRef], searchCriteria: util.Map[String, AnyRef], textFiltersMap: util.Map[String, AnyRef], limit: Int): List[AnyRef] = {
    val result = search(searchCriteria, textFiltersMap, null, false, limit)
    getDocumentsFromSearchResult(result, objectClass)
  }

  def textFiltersGroupBySearch(objectClass: Class[AnyRef], searchCriteria: util.Map[String, AnyRef], textFiltersMap: util.Map[String, AnyRef], groupByList: util.List[util.Map[String, AnyRef]]): util.Map[String, AnyRef] = {
    val result = search(searchCriteria, textFiltersMap, groupByList, false, resultLimit)
    val documents = getDocumentsFromSearchResult(result, objectClass)
    val response = new util.HashMap[String, AnyRef]
    response.put("objects", documents)
    if (result.getAggregations != null) {
      val aggregations = result.getAggregations
      response.put("aggregations", getCountFromAggregation(aggregations, groupByList))
    }
    response
  }

  def textSearch(objectClass: Class[AnyRef], matchCriterias: util.Map[String, AnyRef], textFiltersMap: util.Map[String, AnyRef]): List[AnyRef] = {
    val result = search(matchCriterias, textFiltersMap, null, false, resultLimit)
    getDocumentsFromSearchResult(result, objectClass)
  }

  def textSearch(objectClass: Class[AnyRef], matchCriterias: util.Map[String, AnyRef], textFiltersMap: util.Map[String, AnyRef], groupByList: util.List[util.Map[String, AnyRef]], limit: Int): List[AnyRef] = {
    val result = search(matchCriterias, textFiltersMap, groupByList, false, limit)
    getDocumentsFromSearchResult(result, objectClass)
  }

  def search(matchCriterias: util.Map[String, AnyRef], textFiltersMap: util.Map[String, AnyRef], groupBy: util.List[util.Map[String, AnyRef]], isDistinct: Boolean, limit: Int): SearchResponse = {
    val query = buildJsonForQuery(matchCriterias, textFiltersMap, groupBy, isDistinct)
    query.size(limit)
    search(query)
  }

  def search(query: SearchSourceBuilder): SearchResponse = esClient.search(new SearchRequest(indexName).source(query))

  def count(searchSourceBuilder: SearchSourceBuilder): Int = {
    val response = esClient.search(new SearchRequest().indices(indexName).source(searchSourceBuilder))
    response.getHits.getTotalHits.toInt
  }

  def getCountFromAggregation(aggregations: Aggregations, groupByList: util.List[util.Map[String, AnyRef]]): util.Map[String, AnyRef] = {
    val countMap = new util.HashMap[String, AnyRef]
    if (aggregations != null) {
      groupByList.forEach(aggregationsMap => {
        val parentCountMap = new util.HashMap[String, AnyRef]
        val groupByParent = aggregationsMap.get("groupByParent").asInstanceOf[String]
        val aggKeyMap = aggregations.get(groupByParent).asInstanceOf[util.Map[_, _]]
        val aggKeyList = aggKeyMap.get("buckets").asInstanceOf[util.List[util.Map[String, Double]]]
        val parentGroupList = new util.ArrayList[util.Map[String, AnyRef]]
        aggKeyList.forEach(aggKeyListMap => {
          val parentCountObject = new util.HashMap[String, AnyRef]
          parentCountObject.put("count", aggKeyListMap.get("doc_count").longValue.asInstanceOf[AnyRef])
          val groupByChildList = aggregationsMap.get("groupByChildList").asInstanceOf[util.List[String]]
          if (groupByChildList != null && !groupByChildList.isEmpty) {
            val groupByChildMap = new util.HashMap[String, AnyRef]
            groupByChildList.forEach(groupByChild => {
              val childGroupsList = new util.ArrayList[util.Map[String, Long]]
              val aggChildKeyMap = aggKeyListMap.get(groupByChild).asInstanceOf[util.Map[_, _]]
              val aggChildKeyList = aggChildKeyMap.get("buckets").asInstanceOf[util.List[util.Map[String, Double]]]
              val childCountMap = new util.HashMap[String, Long]
              aggChildKeyList.forEach(aggChildKeyListMap => {
                childCountMap.put(aggChildKeyListMap.get("key").asInstanceOf[String], aggChildKeyListMap.get("doc_count").longValue)
                childGroupsList.add(childCountMap)
                groupByChildMap.put(groupByChild, childCountMap)
              })
            })
            parentCountObject.putAll(groupByChildMap)
          }
          parentCountMap.put(aggKeyListMap.get("key").asInstanceOf[String], parentCountObject)
          parentGroupList.add(parentCountMap)
        })
        countMap.put(groupByParent, parentCountMap)
      })
    }
    countMap
  }

  def getCountOfSearch(objectClass: Class[AnyRef], matchCriterias: util.Map[String, AnyRef], groupByList: util.List[util.Map[String, AnyRef]], limit: Int): util.Map[String, AnyRef] = {
    val result = search(matchCriterias, null, groupByList, false, limit)
    val aggregations = result.getAggregations
    getCountFromAggregation(aggregations, groupByList)
  }

  def getDistinctCountOfSearch(matchCriterias: util.Map[String, AnyRef], groupByList: util.List[util.Map[String, AnyRef]]): util.Map[String, AnyRef] = {
    val countMap = new util.HashMap[String, AnyRef]
    val result = search(matchCriterias, null, groupByList, true, 0)
    val aggregations = result.getAggregations
    if (aggregations != null) {
      groupByList.forEach(aggregationsMap => {
        val parentCountMap = new util.HashMap[String, AnyRef]
        val groupByParent = aggregationsMap.get("groupBy").asInstanceOf[String]
        val aggKeyMap = aggregations.get(groupByParent).asInstanceOf[util.Map[String, AnyRef]]
        val aggKeyList = aggKeyMap.get("buckets").asInstanceOf[util.List[util.Map[String, Double]]]
        aggKeyList.forEach(aggKeyListMap => {
          val distinctKey = aggregationsMap.get("distinctKey").asInstanceOf[String]
          val aggChildKeyMap = aggKeyListMap.get("distinct_" + distinctKey + "s").asInstanceOf[util.Map[String, AnyRef]]
          val count = aggChildKeyMap.get("value").asInstanceOf[Double].longValue
          val keyAsString = aggKeyListMap.get("key_as_string").asInstanceOf[String]
          if (keyAsString != null) parentCountMap.put(keyAsString, count.asInstanceOf[AnyRef])
          else parentCountMap.put(aggKeyListMap.get("key").asInstanceOf[String], count.asInstanceOf[AnyRef])
        })
        countMap.put(groupByParent, parentCountMap)
      })
    }
    countMap
  }

  def buildJsonForQuery(matchCriterias: util.Map[String, AnyRef], textFiltersMap: util.Map[String, AnyRef], groupByList: util.List[util.Map[String, AnyRef]], isDistinct: Boolean): SearchSourceBuilder = {
    val searchSourceBuilder = new SearchSourceBuilder
    val queryBuilder = QueryBuilders.boolQuery
    if (matchCriterias != null) {
      matchCriterias.entrySet.forEach(entry => {
        if (entry.getValue.isInstanceOf[util.List[String]]) {
          entry.getValue.asInstanceOf[util.ArrayList[String]].forEach(matchText => {
            queryBuilder.should(QueryBuilders.matchQuery(entry.getKey, matchText))
          })
        }
      })
    }
    if (textFiltersMap != null && !textFiltersMap.isEmpty) {
      val boolQuery = QueryBuilders.boolQuery
      textFiltersMap.entrySet.forEach(entry => {
        entry.getValue.asInstanceOf[util.ArrayList[String]].forEach(termValue => {
          boolQuery.must(QueryBuilders.termQuery(entry.getKey, termValue))
        })
      })
      queryBuilder.filter(boolQuery)
    }
    searchSourceBuilder.query(QueryBuilders.boolQuery.filter(queryBuilder))
    if (groupByList != null && !groupByList.isEmpty) if (!isDistinct) {
      groupByList.forEach(groupByMap => {
        val groupByParent = groupByMap.get("groupByParent").asInstanceOf[String]
        val groupByChildList = groupByMap.get("groupByChildList").asInstanceOf[util.List[String]]
        val termBuilder = AggregationBuilders.terms(groupByParent).field(groupByParent)
        if (groupByChildList != null && !groupByChildList.isEmpty) {
          groupByChildList.forEach(childGroupBy => {
            termBuilder.subAggregation(AggregationBuilders.terms(childGroupBy).field(childGroupBy))
          })
        }
        searchSourceBuilder.aggregation(termBuilder)
      })
    }
    else {
      groupByList.forEach(groupByMap => {
        val groupBy = groupByMap.get("groupBy").asInstanceOf[String]
        val distinctKey = groupByMap.get("distinctKey").asInstanceOf[String]
        searchSourceBuilder.aggregation(AggregationBuilders.terms(groupBy).field(groupBy).subAggregation(AggregationBuilders.cardinality("distinct_" + distinctKey + "s").field(distinctKey)))
      })
    }
    searchSourceBuilder
  }

  private def buildJsonForWildCardQuery(textKeyWord: String, wordWildCard: String) = new SearchSourceBuilder().query(QueryBuilders.wildcardQuery(textKeyWord, wordWildCard))

  def getCountFromAggregation(aggregations: Aggregations, groupByList: util.List[util.Map[String, AnyRef]], transformer: IESResultTransformer): Any = {
    val countMap = new util.HashMap[String, AnyRef]
    if (aggregations != null) {
      groupByList.forEach(aggregationsMap => {
        val parentCountMap = new util.HashMap[String, AnyRef]
        val groupByParent = aggregationsMap.get("groupByParent").asInstanceOf[String]
        val parentGroupList = new util.ArrayList[util.Map[String, AnyRef]]
        aggregations.get[Terms](groupByParent).getBuckets.asInstanceOf[util.List[Terms.Bucket]]
          .forEach(bucket => {
            val parentCountObject = new util.HashMap[String, AnyRef]
            parentCountObject.put("count", bucket.getDocCount.asInstanceOf[AnyRef])
            val groupByChildList = aggregationsMap.get("groupByChildList").asInstanceOf[util.List[String]]
            val subAggregations = bucket.getAggregations
            if (null != groupByChildList && !groupByChildList.isEmpty && null != subAggregations) {
              val groupByChildMap = new util.HashMap[String, AnyRef]
              groupByChildList.forEach(groupByChild => {
                val childBuckets = subAggregations.get[Terms](groupByChild).getBuckets.asInstanceOf[util.List[Terms.Bucket]]
                val childCountMap = new util.HashMap[String, Long]
                childBuckets.forEach(childBucket => {
                  childCountMap.put(childBucket.getKeyAsString, childBucket.getDocCount)
                  groupByChildMap.put(groupByChild, childCountMap)
                })
              })
              parentCountObject.putAll(groupByChildMap)
            }
            parentCountMap.put(bucket.getKeyAsString, parentCountObject)
            parentGroupList.add(parentCountMap)
          })
        countMap.put(groupByParent, parentCountMap)
      })
    }
    transformer.getTransformedObject(countMap)
  }

  def bulkDeleteDocumentById(identifiers: util.List[String]): Unit = {
    if (isIndexExists()) if (null != identifiers && !identifiers.isEmpty) {
      var count = 0
      val request = new BulkRequest
      identifiers.forEach(documentId => {
        count += 1
        request.add(new DeleteRequest(indexName, indexType, documentId))
        if (count % batchSize == 0 || (count % batchSize < batchSize && count == identifiers.size)) {
          val bulkResponse = esClient.bulk(request)
          val failedIds = bulkResponse.getItems.toStream
            .filter((itemResp: BulkItemResponse) => !StringUtils.equals(itemResp.getResponse[DocWriteResponse].getResult.getLowercase, "deleted"))
            .map((r: BulkItemResponse) => r.getResponse[DocWriteResponse].getId)
            .toList
          if (failedIds.nonEmpty) logger.info("Failed Id's While Deleting Elasticsearch Documents (Bulk Delete) : " + failedIds)
          if (bulkResponse.hasFailures) {
            logger.info(s"Error Occured While Deleting Elasticsearch Documents in Bulk : ${bulkResponse.buildFailureMessage}")
          }
        }
      })
    }
    else throw new Exception(s"ES Index Not Found for BULK_DELETE_ES_DATA With Id : $indexName")
  }

  def close(): Unit = {
    if (null != esClient) try esClient.close()
    catch {
      case e: IOException => e.printStackTrace()
    }
  }

}

trait IESResultTransformer {
  def getTransformedObject(obj: Any): Any
}
