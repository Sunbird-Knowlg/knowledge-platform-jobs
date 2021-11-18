package org.sunbird.job.mvcindexer.task

import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.sunbird.job.BaseJobConfig

class MVCIndexerConfig(override val config: Config) extends BaseJobConfig(config, "mvc-indexer") {

  private val serialVersionUID = 2905979434303791379L

  implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

  // Kafka Topics Configuration
  val kafkaInputTopic: String = config.getString("kafka.input.topic")
  override val kafkaConsumerParallelism: Int = config.getInt("task.consumer.parallelism")
  override val parallelism: Int = config.getInt("task.parallelism")
  val kafkaFailedTopic: String = config.getString("kafka.output.failed.topic")

  // Metric List
  val totalEventsCount = "total-events-count"
  val successEventCount = "success-events-count"
  val failedEventCount = "failed-events-count"
  val esFailedEventCount = "elasticsearch-failed-events-count"
  val skippedEventCount = "skipped-events-count"
  val dbUpdateCount = "db-update-count"
  val dbUpdateFailedCount = "db-update-failed-count"
  val apiFailedEventCount = "api-failed-events-count"

  // Consumers
  val eventConsumer = "mvc-indexer-consumer"
  val mvcIndexerFunction = "mvc-indexer-function"

  // ES Configs
  val esConnectionInfo: String = config.getString("es.basePath")
  val nestedFields: Array[String] = config.getString("nested.fields").split(",")
  val mvcProcessorIndex = "mvc-content-v1"
  val mvcProcessorIndexType = "_doc"
  val operationCreate = "CREATE"
  val indexAlias: String = config.getString("es.indexAlias")
  val indexSettings = """{"max_ngram_diff":"29","mapping":{"total_fields":{"limit":"1500"}},"number_of_shards":"5","analysis":{"filter":{"mynGram":{"token_chars":["letter","digit","whitespace","punctuation","symbol"],"min_gram":"1","type":"nGram","max_gram":"30"}},"analyzer":{"cs_index_analyzer":{"filter":["lowercase","mynGram"],"type":"custom","tokenizer":"standard"},"keylower":{"filter":"lowercase","tokenizer":"keyword"},"ml_custom_analyzer":{"type":"standard","stopwords":["_english_","_hindi_"]},"cs_search_analyzer":{"filter":["lowercase"],"type":"custom","tokenizer":"standard"}}},"number_of_replicas":"1"}"""
  val indexMappings = """{"dynamic":"strict","properties":{"all_fields":{"type":"text","fields":{"raw":{"type":"text","analyzer":"keylower","fielddata":true}},"analyzer":"cs_index_analyzer","search_analyzer":"cs_search_analyzer"},"allowedContentTypes":{"type":"text","fields":{"raw":{"type":"text","analyzer":"keylower","fielddata":true}},"copy_to":["all_fields"],"analyzer":"cs_index_analyzer","search_analyzer":"cs_search_analyzer"},"appIcon":{"type":"text","index":false},"appIconLabel":{"type":"text","fields":{"raw":{"type":"text","analyzer":"keylower","fielddata":true}},"copy_to":["all_fields"],"analyzer":"cs_index_analyzer","search_analyzer":"cs_search_analyzer"},"appId":{"type":"text","fields":{"raw":{"type":"text","analyzer":"keylower","fielddata":true}},"copy_to":["all_fields"],"analyzer":"cs_index_analyzer","search_analyzer":"cs_search_analyzer"},"artifactUrl":{"type":"text","fields":{"raw":{"type":"text","analyzer":"keylower","fielddata":true}},"copy_to":["all_fields"],"analyzer":"cs_index_analyzer","search_analyzer":"cs_search_analyzer"},"board":{"type":"text","fields":{"raw":{"type":"text","analyzer":"keylower","fielddata":true}},"copy_to":["all_fields"],"analyzer":"cs_index_analyzer","search_analyzer":"cs_search_analyzer"},"channel":{"type":"text","fields":{"raw":{"type":"text","analyzer":"keylower","fielddata":true}},"copy_to":["all_fields"],"analyzer":"cs_index_analyzer","search_analyzer":"cs_search_analyzer"},"contentEncoding":{"type":"text","fields":{"raw":{"type":"text","analyzer":"keylower","fielddata":true}},"copy_to":["all_fields"],"analyzer":"cs_index_analyzer","search_analyzer":"cs_search_analyzer"},"contentType":{"type":"text","fields":{"raw":{"type":"text","analyzer":"keylower","fielddata":true}},"copy_to":["all_fields"],"analyzer":"cs_index_analyzer","search_analyzer":"cs_search_analyzer"},"description":{"type":"text","fields":{"raw":{"type":"text","analyzer":"keylower","fielddata":true}},"copy_to":["all_fields"],"analyzer":"cs_index_analyzer","search_analyzer":"cs_search_analyzer"},"downloadUrl":{"type":"text","fields":{"raw":{"type":"text","analyzer":"keylower","fielddata":true}},"copy_to":["all_fields"],"analyzer":"cs_index_analyzer","search_analyzer":"cs_search_analyzer"},"framework":{"type":"text","fields":{"raw":{"type":"text","analyzer":"keylower","fielddata":true}},"copy_to":["all_fields"],"analyzer":"cs_index_analyzer","search_analyzer":"cs_search_analyzer"},"gradeLevel":{"type":"text","fields":{"raw":{"type":"text","analyzer":"keylower","fielddata":true}},"copy_to":["all_fields"],"analyzer":"cs_index_analyzer","search_analyzer":"cs_search_analyzer"},"identifier":{"type":"text","fields":{"raw":{"type":"text","analyzer":"keylower","fielddata":true}},"copy_to":["all_fields"],"analyzer":"cs_index_analyzer","search_analyzer":"cs_search_analyzer"},"label":{"type":"text","analyzer":"cs_index_analyzer","search_analyzer":"standard"},"language":{"type":"text","fields":{"raw":{"type":"text","analyzer":"keylower","fielddata":true}},"copy_to":["all_fields"],"analyzer":"cs_index_analyzer","search_analyzer":"cs_search_analyzer"},"lastUpdatedOn":{"type":"date","fields":{"raw":{"type":"date"}}},"launchUrl":{"type":"text","fields":{"raw":{"type":"text","analyzer":"keylower","fielddata":true}},"copy_to":["all_fields"],"analyzer":"cs_index_analyzer","search_analyzer":"cs_search_analyzer"},"level1Concept":{"type":"text","fields":{"raw":{"type":"text","analyzer":"keylower","fielddata":true}},"copy_to":["all_fields"],"analyzer":"cs_index_analyzer","search_analyzer":"cs_search_analyzer"},"level1Name":{"type":"text","fields":{"raw":{"type":"text","analyzer":"keylower","fielddata":true}},"copy_to":["all_fields"],"analyzer":"cs_index_analyzer","search_analyzer":"cs_search_analyzer"},"level2Concept":{"type":"text","fields":{"raw":{"type":"text","analyzer":"keylower","fielddata":true}},"copy_to":["all_fields"],"analyzer":"cs_index_analyzer","search_analyzer":"cs_search_analyzer"},"level2Name":{"type":"text","fields":{"raw":{"type":"text","analyzer":"keylower","fielddata":true}},"copy_to":["all_fields"],"analyzer":"cs_index_analyzer","search_analyzer":"cs_search_analyzer"},"level3Concept":{"type":"text","fields":{"raw":{"type":"text","analyzer":"keylower","fielddata":true}},"copy_to":["all_fields"],"analyzer":"cs_index_analyzer","search_analyzer":"cs_search_analyzer"},"level3Name":{"type":"text","fields":{"raw":{"type":"text","analyzer":"keylower","fielddata":true}},"copy_to":["all_fields"],"analyzer":"cs_index_analyzer","search_analyzer":"cs_search_analyzer"},"medium":{"type":"text","fields":{"raw":{"type":"text","analyzer":"keylower","fielddata":true}},"copy_to":["all_fields"],"analyzer":"cs_index_analyzer","search_analyzer":"cs_search_analyzer"},"mimeType":{"type":"text","fields":{"raw":{"type":"text","analyzer":"keylower","fielddata":true}},"copy_to":["all_fields"],"analyzer":"cs_index_analyzer","search_analyzer":"cs_search_analyzer"},"ml_Keywords":{"type":"text","analyzer":"ml_custom_analyzer","search_analyzer":"standard"},"ml_contentText":{"type":"text","analyzer":"ml_custom_analyzer","search_analyzer":"standard"},"ml_contentTextVector":{"type":"long"},"name":{"type":"text","fields":{"raw":{"type":"text","analyzer":"keylower","fielddata":true}},"copy_to":["all_fields"],"analyzer":"cs_index_analyzer","search_analyzer":"cs_search_analyzer"},"nodeType":{"type":"text","fields":{"raw":{"type":"text","analyzer":"keylower","fielddata":true}},"copy_to":["all_fields"],"analyzer":"cs_index_analyzer","search_analyzer":"cs_search_analyzer"},"node_id":{"type":"long","fields":{"raw":{"type":"long"}}},"objectType":{"type":"text","fields":{"raw":{"type":"text","analyzer":"keylower","fielddata":true}},"copy_to":["all_fields"],"analyzer":"cs_index_analyzer","search_analyzer":"cs_search_analyzer"},"organisation":{"type":"text","fields":{"raw":{"type":"text","analyzer":"keylower","fielddata":true}},"copy_to":["all_fields"],"analyzer":"cs_index_analyzer","search_analyzer":"cs_search_analyzer"},"pkgVersion":{"type":"double","fields":{"raw":{"type":"double"}}},"posterImage":{"type":"text","fields":{"raw":{"type":"text","analyzer":"keylower","fielddata":true}},"copy_to":["all_fields"],"analyzer":"cs_index_analyzer","search_analyzer":"cs_search_analyzer"},"previewUrl":{"type":"text","fields":{"raw":{"type":"text","analyzer":"keylower","fielddata":true}},"copy_to":["all_fields"],"analyzer":"cs_index_analyzer","search_analyzer":"cs_search_analyzer"},"resourceType":{"type":"text","fields":{"raw":{"type":"text","analyzer":"keylower","fielddata":true}},"copy_to":["all_fields"],"analyzer":"cs_index_analyzer","search_analyzer":"cs_search_analyzer"},"source":{"type":"text","fields":{"raw":{"type":"text","analyzer":"keylower","fielddata":true}},"copy_to":["all_fields"],"analyzer":"cs_index_analyzer","search_analyzer":"cs_search_analyzer"},"sourceURL":{"type":"text","fields":{"raw":{"type":"text","analyzer":"keylower","fielddata":true}},"copy_to":["all_fields"],"analyzer":"cs_index_analyzer","search_analyzer":"cs_search_analyzer"},"status":{"type":"text","fields":{"raw":{"type":"text","analyzer":"keylower","fielddata":true}},"copy_to":["all_fields"],"analyzer":"cs_index_analyzer","search_analyzer":"cs_search_analyzer"},"streamingUrl":{"type":"text","fields":{"raw":{"type":"text","analyzer":"keylower","fielddata":true}},"copy_to":["all_fields"],"analyzer":"cs_index_analyzer","search_analyzer":"cs_search_analyzer"},"subject":{"type":"text","fields":{"raw":{"type":"text","analyzer":"keylower","fielddata":true}},"copy_to":["all_fields"],"analyzer":"cs_index_analyzer","search_analyzer":"cs_search_analyzer"},"textbook_name":{"type":"text","fields":{"raw":{"type":"text","analyzer":"keylower","fielddata":true}},"copy_to":["all_fields"],"analyzer":"cs_index_analyzer","search_analyzer":"cs_search_analyzer"}}}"""
  val elasticSearchParamSet = Set("organisation", "channel", "framework", "board", "medium", "subject", "gradeLevel", "name", "description", "language", "appId", "appIcon", "appIconLabel", "contentEncoding", "identifier", "node_id", "nodeType", "mimeType", "resourceType", "contentType", "allowedContentTypes", "objectType", "posterImage", "artifactUrl", "launchUrl", "previewUrl", "streamingUrl", "downloadUrl", "status", "pkgVersion", "source", "lastUpdatedOn", "ml_contentText", "ml_contentTextVector", "ml_Keywords", "level1Name", "level1Concept", "level2Name", "level2Concept", "level3Name", "level3Concept", "textbook_name", "sourceURL", "label", "all_fields")

  // API Configs
  val contentServiceBase: String = config.getString("service.content.basePath")
  val contentReadURL = s"${contentServiceBase}/content/v3/read/"
  val mlVectorAPIUrl = s"http://${config.getString("ml.vector.host")}:${config.getString("ml.vector.port")}/ml/vector/ContentText"
  val mlKeywordAPIUrl = s"http://${config.getString("ml.keyword.host")}:${config.getString("ml.keyword.port")}/daggit/submit"
  val keywordAPIJobname = "vidyadaan_content_keyword_tagging"

  // Cassandra Configurations
  val dbTable: String = config.getString("lms-cassandra.table")
  val dbKeyspace: String = config.getString("lms-cassandra.keyspace")
  val csFieldMap: Map[String, String] = Map[String, String]("level1Concept" -> "level1_concept", "level2Concept" -> "level2_concept",
    "level3Concept" -> "level3_concept", "textbook_name" -> "textbook_name", "level1Name" -> "level1_name",
    "level2Name" -> "level2_name", "level3Name" -> "level3_name")


  // Actions
  val esIndexAction = "update-es-index"
  val contentRatingAction = "update-content-rating"
  val mlVectorAction = "update-ml-contenttextvector"
  val mlKeywordAction = "update-ml-keywords"
  val csUpdateAcions: Array[String] = Array(esIndexAction, mlVectorAction, mlKeywordAction)
}
