package org.sunbird.job.spec

import java.util

import scala.collection.JavaConverters._
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.mockito.ArgumentMatchers.anyString
import org.mockito.Mockito
import org.mockito.Mockito.{doNothing, times, verify, when}
import org.sunbird.job.compositesearch.domain.Event
import org.sunbird.job.connector.FlinkKafkaConnector
import org.sunbird.job.fixture.EventFixture
import org.sunbird.job.functions.{CompositeSearchIndexerFunction, DialCodeExternalIndexerFunction, DialCodeMetricIndexerFunction}
import org.sunbird.job.task.CompositeSearchIndexerConfig
import org.sunbird.job.util.{DefinitionUtil, ElasticSearchUtil, ScalaJsonUtil}
import org.sunbird.spec.BaseTestSpec

class CompositeSearchIndexerTaskTestSpec extends BaseTestSpec {

  implicit val mapTypeInfo: TypeInformation[java.util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[java.util.Map[String, AnyRef]])
  implicit val strTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

  val flinkCluster = new MiniClusterWithClientResource(new MiniClusterResourceConfiguration.Builder()
    .setConfiguration(testConfiguration())
    .setNumberSlotsPerTaskManager(1)
    .setNumberTaskManagers(1)
    .build)
  val mockKafkaUtil: FlinkKafkaConnector = mock[FlinkKafkaConnector](Mockito.withSettings().serializable())
  val config: Config = ConfigFactory.load("test.conf")
  val jobConfig = new CompositeSearchIndexerConfig(config)

  val mockElasticutil = mock[ElasticSearchUtil](Mockito.withSettings().serializable())

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    flinkCluster.before()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    flinkCluster.after()
  }

  "getCompositeIndexerobject" should " return Composite Object for the event" in {
    val event = getEvent(EventFixture.DATA_NODE_CREATE, 509674)
    val compositeObject = new CompositeSearchIndexerFunction(jobConfig).getCompositeIndexerobject(event)
    compositeObject.objectType should be("Collection")
    compositeObject.getVersionAsString() should be("1.0")
    compositeObject.uniqueId should be("do_1132247274257203201191")
  }

  "getDefinition" should "return the definition for the object type" in {
    val definition = new DefinitionUtil().getDefinition("Collection", "1.0", jobConfig.definitionBasePath)
    val schema = definition.getOrElse("schema", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
    val config = definition.getOrElse("config", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
    schema.isEmpty should be(false)
    config.isEmpty should be(false)
    config.getOrElse("objectType", "").asInstanceOf[String] should be("Collection")
  }

  "retrieveRelations" should "return the relation map from the definiiton object" in {
    val definition = new DefinitionUtil().getDefinition("Collection", "1.0", jobConfig.definitionBasePath)
    val compositeFunc = new CompositeSearchIndexerFunction(jobConfig)
    val relations = compositeFunc.retrieveRelations(definition)
    relations.isEmpty should be(false)
    relations.getOrElse("IN_Content_hasSequenceMember", "").asInstanceOf[String] should be("collections")
    relations.getOrElse("OUT_ContentImage_hasSequenceMember", "").asInstanceOf[String] should be("children")
  }

  "retrieveExternalProperties" should "return the list of external properties from the definiiton object" in {
    val definition = new DefinitionUtil().getDefinition("Collection", "1.0", jobConfig.definitionBasePath)
    val compositeFunc = new CompositeSearchIndexerFunction(jobConfig)
    val external = compositeFunc.retrieveExternalProperties(definition)
    external.isEmpty should be(false)
    external should contain("hierarchy")
  }

  "getIndexableProperties" should "return the list of indexable properties from the definiiton object" in {
    val definition = new DefinitionUtil().getDefinition("Collection", "1.0", jobConfig.definitionBasePath)
    val compositeFunc = new CompositeSearchIndexerFunction(jobConfig)
    val indexable = compositeFunc.getIndexableProperties(definition)
    indexable.isEmpty should be(true)
  }

  "getIndexDocument" should "return the indexable document for the provided object" in {
    val definition = new DefinitionUtil().getDefinition("Collection", "1.0", jobConfig.definitionBasePath)
    val compositeFunc = new CompositeSearchIndexerFunction(jobConfig)
    val message = getEvent(EventFixture.DATA_NODE_CREATE, 509674).getMap().asScala.toMap
    val relations = compositeFunc.retrieveRelations(definition)
    val external = compositeFunc.retrieveExternalProperties(definition)
    val indexable = compositeFunc.getIndexableProperties(definition)
    val indexDocument: Map[String, AnyRef] = compositeFunc.getIndexDocument(message, relations, false, external, indexable, jobConfig.nestedFields.asScala.toList)(mockElasticutil)
    val trackable = indexDocument.getOrElse("trackable", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
    indexDocument.isEmpty should be(false)
    indexDocument.getOrElse("identifier", "").asInstanceOf[String] should be("do_1132247274257203201191")
    trackable.getOrElse("enabled", "") should be("No")
    indexDocument.getOrElse("objectType", "").asInstanceOf[String] should be("Collection")
  }

  "getIndexDocument" should "return the indexable document with the added relation for the provided object" in {
    val definition = new DefinitionUtil().getDefinition("Collection", "1.0", jobConfig.definitionBasePath)
    val compositeFunc = new CompositeSearchIndexerFunction(jobConfig)
    val message = getEvent(EventFixture.DATA_NODE_CREATE_WITH_RELATION, 509674).getMap().asScala.toMap
    val relations = compositeFunc.retrieveRelations(definition)
    val external = compositeFunc.retrieveExternalProperties(definition)
    val indexable = compositeFunc.getIndexableProperties(definition)
    val indexDocument: Map[String, AnyRef] = compositeFunc.getIndexDocument(message, relations, false, external, indexable, jobConfig.nestedFields.asScala.toList)(mockElasticutil)
    indexDocument.isEmpty should be(false)
    indexDocument.getOrElse("identifier", "").asInstanceOf[String] should be("do_112276071067320320114")
    indexDocument.getOrElse("objectType", "").asInstanceOf[String] should be("Content")
    indexDocument.getOrElse("collections", List[String]()).asInstanceOf[List[String]] should contain("do_1123032073439723521148")
  }

  "getIndexDocument" should "return the indexable document without the deleted relation for the provided object" in {
    val documentJson = """{"identifier":"do_112276071067320320114","graph_id":"domain","node_id":105631,"collections":["do_1123032073439723521148", "do_1123032073439723521149"],"objectType":"Content","nodeType":"DATA_NODE"}"""
    when(mockElasticutil.getDocumentAsStringById(anyString())).thenReturn(documentJson)

    val definition = new DefinitionUtil().getDefinition("Collection", "1.0", jobConfig.definitionBasePath)
    val compositeFunc = new CompositeSearchIndexerFunction(jobConfig)
    val message = getEvent(EventFixture.DATA_NODE_UPDATE_WITH_RELATION, 509674).getMap().asScala.toMap
    val relations = compositeFunc.retrieveRelations(definition)
    val external = compositeFunc.retrieveExternalProperties(definition)
    val indexable = compositeFunc.getIndexableProperties(definition)
    val indexDocument: Map[String, AnyRef] = compositeFunc.getIndexDocument(message, relations, true, external, indexable, jobConfig.nestedFields.asScala.toList)(mockElasticutil)
    val collections = indexDocument.getOrElse("collections", List[String]()).asInstanceOf[List[String]]
    indexDocument.isEmpty should be(false)
    indexDocument.getOrElse("identifier", "").asInstanceOf[String] should be("do_112276071067320320114")
    indexDocument.getOrElse("objectType", "").asInstanceOf[String] should be("Content")
    collections.length should be(1)
    collections should contain("do_1123032073439723521149")
    indexDocument.getOrElse("collections", List[String]()).asInstanceOf[List[String]] should not contain  ("do_1123032073439723521148")
  }

  "getIndexDocument" should "return the indexable document for the provided update object" in {
    val documentJson = """{"ownershipType":["createdBy"],"code":"org.sunbird.zf7fcK","credentials":{"enabled":"No"},"subject":["Geography"],"channel":"channel-1","language":["English"],"mimeType":"application/vnd.ekstep.content-collection","idealScreenSize":"normal","createdOn":"2021-02-26T13:36:49.592+0000","objectType":"Collection","primaryCategory":"Digital Textbook","contentDisposition":"inline","additionalCategories":["Textbook"],"lastUpdatedOn":"2021-02-26T13:36:49.592+0000","contentEncoding":"gzip","dialcodeRequired":"No","contentType":"TextBook","trackable":{"enabled":"No","autoBatch":"No"},"identifier":"do_1132247274257203201191","subjectIds":["ncf_subject_geography"],"lastStatusChangedOn":"2021-02-26T13:36:49.592+0000","audience":["Student"],"IL_SYS_NODE_TYPE":"DATA_NODE","os":["All"],"visibility":"Default","consumerId":"7411b6bd-89f3-40ec-98d1-229dc64ce77d","mediaType":"content","osId":"org.ekstep.quiz.app","graph_id":"domain","nodeType":"DATA_NODE","version":2,"versionKey":"1614346609592","idealScreenDensity":"hdpi","license":"CC BY-SA 4.0","framework":"NCF","createdBy":"95e4942d-cbe8-477d-aebd-ad8e6de4bfc8","compatibilityLevel":1,"IL_FUNC_OBJECT_TYPE":"Collection","userConsent":"Yes","name":"Test","IL_UNIQUE_ID":"do_1132247274257203201191","status":"Draft","node_id":509674}"""
    when(mockElasticutil.getDocumentAsStringById(anyString())).thenReturn(documentJson)

    val definition = new DefinitionUtil().getDefinition("Collection", "1.0", jobConfig.definitionBasePath)
    val compositeFunc = new CompositeSearchIndexerFunction(jobConfig)
    val message = getEvent(EventFixture.DATA_NODE_UPDATE, 509674).getMap().asScala.toMap
    val relations = compositeFunc.retrieveRelations(definition)
    val external = compositeFunc.retrieveExternalProperties(definition)
    val indexable = compositeFunc.getIndexableProperties(definition)
    val indexDocument: Map[String, AnyRef] = compositeFunc.getIndexDocument(message, relations, true, external, indexable, jobConfig.nestedFields.asScala.toList)(mockElasticutil)
    indexDocument.isEmpty should be(false)
    indexDocument.getOrElse("identifier", "").asInstanceOf[String] should be("do_1132247274257203201191")
    indexDocument.getOrElse("objectType", "").asInstanceOf[String] should be("Collection")
    indexDocument.getOrElse("description", "").asInstanceOf[String] should be("updated description")
  }

  "getIndexDocument " should " give the document for indexing the dialcode metrics " in {
    val documentJson = """{"last_scan":1541456052000,"dial_code":"QR1234","first_scan":1540469152000,"total_dial_scans_local":25,"objectType":"","average_scans_per_day":2}"""
    when(mockElasticutil.getDocumentAsStringById(anyString())).thenReturn(documentJson)

    val event = getEvent(EventFixture.DIALCODE_METRIC_UPDATE, 509674)
    val dialcodeMetricFunc = new DialCodeMetricIndexerFunction(jobConfig)
    val response = dialcodeMetricFunc.getIndexDocument(event.getMap().asScala.toMap, true)(mockElasticutil)
    response.isEmpty should be(false)
    response.getOrElse("total_dial_scans_global", 0).asInstanceOf[Integer] should be(25)
  }

  "getIndexDocument " should " give the document for indexing the dialcode external " in {
    val event = getEvent(EventFixture.DIALCODE_EXTERNAL_CREATE, 509674)
    val dialcodeExternalFunc = new DialCodeExternalIndexerFunction(jobConfig)
    val response = dialcodeExternalFunc.getIndexDocument(event.getMap().asScala.toMap, false)(mockElasticutil)
    response.isEmpty should be(false)
    response.getOrElse("identifier", "").asInstanceOf[String] should be("X8R3W4")
    response.getOrElse("objectType", "").asInstanceOf[String] should be ("DialCode")
    response.getOrElse("batchcode", "").asInstanceOf[String] should be ("testPub0001.20210212T011555")
  }

  "processESMessage " should " index the event for the appropriate fields" in {
    Mockito.reset(mockElasticutil)
    doNothing().when(mockElasticutil).addDocumentWithId(anyString(), anyString())

    val event = getEvent(EventFixture.DATA_NODE_CREATE, 509674)
    val compositeFunc = new CompositeSearchIndexerFunction(jobConfig)
    val compositeObject = compositeFunc.getCompositeIndexerobject(event)
    compositeFunc.processESMessage(compositeObject)(mockElasticutil)

    verify(mockElasticutil, times(1)).addDocumentWithId(anyString(), anyString())
    verify(mockElasticutil, times(0)).getDocumentAsStringById(anyString())
  }

  "processESMessage " should " update the indexed event for the new values of the properties" in {
    Mockito.reset(mockElasticutil)
    doNothing().when(mockElasticutil).addDocumentWithId(anyString(), anyString())
    val documentJson = """{"ownershipType":["createdBy"],"code":"org.sunbird.zf7fcK","credentials":{"enabled":"No"},"subject":["Geography"],"channel":"channel-1","language":["English"],"mimeType":"application/vnd.ekstep.content-collection","idealScreenSize":"normal","createdOn":"2021-02-26T13:36:49.592+0000","objectType":"Collection","primaryCategory":"Digital Textbook","contentDisposition":"inline","additionalCategories":["Textbook"],"lastUpdatedOn":"2021-02-26T13:36:49.592+0000","contentEncoding":"gzip","dialcodeRequired":"No","contentType":"TextBook","trackable":{"enabled":"No","autoBatch":"No"},"identifier":"do_1132247274257203201191","subjectIds":["ncf_subject_geography"],"lastStatusChangedOn":"2021-02-26T13:36:49.592+0000","audience":["Student"],"IL_SYS_NODE_TYPE":"DATA_NODE","os":["All"],"visibility":"Default","consumerId":"7411b6bd-89f3-40ec-98d1-229dc64ce77d","mediaType":"content","osId":"org.ekstep.quiz.app","graph_id":"domain","nodeType":"DATA_NODE","version":2,"versionKey":"1614346609592","idealScreenDensity":"hdpi","license":"CC BY-SA 4.0","framework":"NCF","createdBy":"95e4942d-cbe8-477d-aebd-ad8e6de4bfc8","compatibilityLevel":1,"IL_FUNC_OBJECT_TYPE":"Collection","userConsent":"Yes","name":"Test","IL_UNIQUE_ID":"do_1132247274257203201191","status":"Draft","node_id":509674}"""
    when(mockElasticutil.getDocumentAsStringById(anyString())).thenReturn(documentJson)

    val event = getEvent(EventFixture.DATA_NODE_UPDATE, 509674)
    val compositeFunc = new CompositeSearchIndexerFunction(jobConfig)
    val compositeObject = compositeFunc.getCompositeIndexerobject(event)
    compositeFunc.processESMessage(compositeObject)(mockElasticutil)

    verify(mockElasticutil, times(1)).addDocumentWithId(anyString(), anyString())
    verify(mockElasticutil, times(1)).getDocumentAsStringById(anyString())
  }

  "processESMessage " should " delete the indexed event " in {
    Mockito.reset(mockElasticutil)
    doNothing().when(mockElasticutil).deleteDocument(anyString())
    val documentJson = """{"ownershipType":["createdBy"],"code":"org.sunbird.zf7fcK","credentials":{"enabled":"No"},"subject":["Geography"],"channel":"channel-1","language":["English"],"mimeType":"application/vnd.ekstep.content-collection","idealScreenSize":"normal","createdOn":"2021-02-26T13:36:49.592+0000","objectType":"Collection","primaryCategory":"Digital Textbook","contentDisposition":"inline","additionalCategories":["Textbook"],"lastUpdatedOn":"2021-02-26T13:36:49.592+0000","contentEncoding":"gzip","dialcodeRequired":"No","contentType":"TextBook","trackable":{"enabled":"No","autoBatch":"No"},"identifier":"do_1132247274257203201191","subjectIds":["ncf_subject_geography"],"lastStatusChangedOn":"2021-02-26T13:36:49.592+0000","audience":["Student"],"IL_SYS_NODE_TYPE":"DATA_NODE","os":["All"],"visibility":"Default","consumerId":"7411b6bd-89f3-40ec-98d1-229dc64ce77d","mediaType":"content","osId":"org.ekstep.quiz.app","graph_id":"domain","nodeType":"DATA_NODE","version":2,"versionKey":"1614346609592","idealScreenDensity":"hdpi","license":"CC BY-SA 4.0","framework":"NCF","createdBy":"95e4942d-cbe8-477d-aebd-ad8e6de4bfc8","compatibilityLevel":1,"IL_FUNC_OBJECT_TYPE":"Collection","userConsent":"Yes","name":"Test","IL_UNIQUE_ID":"do_1132247274257203201191","status":"Draft","node_id":509674}"""
    when(mockElasticutil.getDocumentAsStringById(anyString())).thenReturn(documentJson)

    val event = getEvent(EventFixture.DATA_NODE_DELETE, 509674)
    val compositeFunc = new CompositeSearchIndexerFunction(jobConfig)
    val compositeObject = compositeFunc.getCompositeIndexerobject(event)
    compositeFunc.processESMessage(compositeObject)(mockElasticutil)

    verify(mockElasticutil, times(1)).getDocumentAsStringById(anyString())
    verify(mockElasticutil, times(1)).deleteDocument(anyString())
  }

  "processESMessage " should " not delete the indexed event with visibility Parent " in {
    Mockito.reset(mockElasticutil)
    val documentJson = """{"ownershipType":["createdBy"],"code":"org.sunbird.zf7fcK","credentials":{"enabled":"No"},"subject":["Geography"],"channel":"channel-1","language":["English"],"mimeType":"application/vnd.ekstep.content-collection","idealScreenSize":"normal","createdOn":"2021-02-26T13:36:49.592+0000","objectType":"Collection","primaryCategory":"Digital Textbook","contentDisposition":"inline","additionalCategories":["Textbook"],"lastUpdatedOn":"2021-02-26T13:36:49.592+0000","contentEncoding":"gzip","dialcodeRequired":"No","contentType":"TextBook","trackable":{"enabled":"No","autoBatch":"No"},"identifier":"do_1132247274257203201191","subjectIds":["ncf_subject_geography"],"lastStatusChangedOn":"2021-02-26T13:36:49.592+0000","audience":["Student"],"IL_SYS_NODE_TYPE":"DATA_NODE","os":["All"],"visibility":"Parent","consumerId":"7411b6bd-89f3-40ec-98d1-229dc64ce77d","mediaType":"content","osId":"org.ekstep.quiz.app","graph_id":"domain","nodeType":"DATA_NODE","version":2,"versionKey":"1614346609592","idealScreenDensity":"hdpi","license":"CC BY-SA 4.0","framework":"NCF","createdBy":"95e4942d-cbe8-477d-aebd-ad8e6de4bfc8","compatibilityLevel":1,"IL_FUNC_OBJECT_TYPE":"Collection","userConsent":"Yes","name":"Test","IL_UNIQUE_ID":"do_1132247274257203201191","status":"Draft","node_id":509674}"""
    when(mockElasticutil.getDocumentAsStringById(anyString())).thenReturn(documentJson)

    val event = getEvent(EventFixture.DATA_NODE_DELETE, 509674)
    val compositeFunc = new CompositeSearchIndexerFunction(jobConfig)
    val compositeObject = compositeFunc.getCompositeIndexerobject(event)
    compositeFunc.processESMessage(compositeObject)(mockElasticutil)

    verify(mockElasticutil, times(1)).getDocumentAsStringById(anyString())
    verify(mockElasticutil, times(0)).deleteDocument(anyString())
  }

  "processESMessage " should " index the event with the added Relations" in {
    Mockito.reset(mockElasticutil)
    doNothing().when(mockElasticutil).addDocumentWithId(anyString(), anyString())

    val event = getEvent(EventFixture.DATA_NODE_CREATE_WITH_RELATION, 509674)
    val compositeFunc = new CompositeSearchIndexerFunction(jobConfig)
    val compositeObject = compositeFunc.getCompositeIndexerobject(event)
    compositeFunc.processESMessage(compositeObject)(mockElasticutil)

    verify(mockElasticutil, times(1)).addDocumentWithId(anyString(), anyString())
    verify(mockElasticutil, times(0)).getDocumentAsStringById(anyString())
  }

  "upsertDialcodeMetricDocument " should " index the dialcode metrics " in {
    Mockito.reset(mockElasticutil)
    doNothing().when(mockElasticutil).addDocumentWithId(anyString(), anyString())

    val event = getEvent(EventFixture.DIALCODE_METRIC_CREATE, 509674)
    val dialcodeMetricFunc = new DialCodeMetricIndexerFunction(jobConfig)
    val uniqueId = event.readOrDefault("nodeUniqueId", "")
    dialcodeMetricFunc.upsertDialcodeMetricDocument(uniqueId, event.getMap().asScala.toMap)(mockElasticutil)

    verify(mockElasticutil, times(1)).addDocumentWithId(anyString(), anyString())
    verify(mockElasticutil, times(0)).getDocumentAsStringById(anyString())
  }

  "upsertDialcodeMetricDocument " should " update the indexed the dialcode metrics " in {
    Mockito.reset(mockElasticutil)
    doNothing().when(mockElasticutil).addDocumentWithId(anyString(), anyString())
    val documentJson = """{"last_scan":1541456052000,"dial_code":"QR1234","first_scan":1540469152000,"total_dial_scans_local":25,"objectType":"","average_scans_per_day":2}"""
    when(mockElasticutil.getDocumentAsStringById(anyString())).thenReturn(documentJson)

    val event = getEvent(EventFixture.DIALCODE_METRIC_UPDATE, 509674)
    val dialcodeMetricFunc = new DialCodeMetricIndexerFunction(jobConfig)
    val uniqueId = event.readOrDefault("nodeUniqueId", "")
    dialcodeMetricFunc.upsertDialcodeMetricDocument(uniqueId, event.getMap().asScala.toMap)(mockElasticutil)

    verify(mockElasticutil, times(1)).addDocumentWithId(anyString(), anyString())
    verify(mockElasticutil, times(1)).getDocumentAsStringById(anyString())
  }

  "upsertDialcodeMetricDocument " should " delete the indexed dialcode metric event " in {
    Mockito.reset(mockElasticutil)
    val event = getEvent(EventFixture.DIALCODE_METRIC_DELETE, 509674)
    val dialcodeMetricFunc = new DialCodeMetricIndexerFunction(jobConfig)
    val uniqueId = event.readOrDefault("nodeUniqueId", "")
    dialcodeMetricFunc.upsertDialcodeMetricDocument(uniqueId, event.getMap().asScala.toMap)(mockElasticutil)

    verify(mockElasticutil, times(0)).getDocumentAsStringById(anyString())
    verify(mockElasticutil, times(1)).deleteDocument(anyString())
  }

  "upsertExternalDocument " should " index the dialcode external " in {
    Mockito.reset(mockElasticutil)
    doNothing().when(mockElasticutil).addDocumentWithId(anyString(), anyString())

    val event = getEvent(EventFixture.DIALCODE_EXTERNAL_CREATE, 509674)
    val dialcodeExternalFunc = new DialCodeExternalIndexerFunction(jobConfig)
    val uniqueId = event.readOrDefault("nodeUniqueId", "")
    dialcodeExternalFunc.upsertExternalDocument(uniqueId, event.getMap().asScala.toMap)(mockElasticutil)

    verify(mockElasticutil, times(1)).addDocumentWithId(anyString(), anyString())
    verify(mockElasticutil, times(0)).getDocumentAsStringById(anyString())
  }

  "upsertExternalDocument " should " update and index the dialcode external " in {
    Mockito.reset(mockElasticutil)
    doNothing().when(mockElasticutil).addDocumentWithId(anyString(), anyString())
    val documentJson = """{"channel":"channelTest","generated_on":"2021-02-12T01:16:07.750+0530","identifier":"X8R3W4","dialcode_index":9071809.0,"batchcode":"testPub0001.20210212T011555","objectType":"DialCode","status":"Draft","publisher":"testPub0001"}"""
    when(mockElasticutil.getDocumentAsStringById(anyString())).thenReturn(documentJson)

    val event = getEvent(EventFixture.DIALCODE_EXTERNAL_UPDATE, 509674)
    val dialcodeExternalFunc = new DialCodeExternalIndexerFunction(jobConfig)
    val uniqueId = event.readOrDefault("nodeUniqueId", "")
    dialcodeExternalFunc.upsertExternalDocument(uniqueId, event.getMap().asScala.toMap)(mockElasticutil)

    verify(mockElasticutil, times(1)).addDocumentWithId(anyString(), anyString())
    verify(mockElasticutil, times(1)).getDocumentAsStringById(anyString())
  }

  "upsertExternalDocument " should " delete the indexed dialcode external event " in {
    Mockito.reset(mockElasticutil)
    val event = getEvent(EventFixture.DIALCODE_EXTERNAL_DELETE, 509674)
    val dialcodeExternalFunc = new DialCodeExternalIndexerFunction(jobConfig)
    val uniqueId = event.readOrDefault("nodeUniqueId", "")
    dialcodeExternalFunc.upsertExternalDocument(uniqueId, event.getMap().asScala.toMap)(mockElasticutil)

    verify(mockElasticutil, times(0)).getDocumentAsStringById(anyString())
    verify(mockElasticutil, times(1)).deleteDocument(anyString())
  }

  "createCompositeSearchIndex" should "create the elastic search index for compositesearch" in {
    Mockito.reset(mockElasticutil)
    when(mockElasticutil.addIndex(anyString(), anyString())).thenReturn(false)
    val compositeFunc = new CompositeSearchIndexerFunction(jobConfig)
    val check = compositeFunc.createCompositeSearchIndex()(mockElasticutil)
    check should be(false)
  }

  "createDialCodeIndex" should "create the elastic search index for dialcode" in {
    Mockito.reset(mockElasticutil)
    when(mockElasticutil.addIndex(anyString(), anyString())).thenReturn(false)
    val compositeFunc = new DialCodeExternalIndexerFunction(jobConfig)
    val check = compositeFunc.createDialCodeIndex()(mockElasticutil)
    check should be(false)
  }

  "createDialCodeIndex" should "create the elastic search index for dialcode metric" in {
    Mockito.reset(mockElasticutil)
    when(mockElasticutil.addIndex(anyString(), anyString())).thenReturn(false)
    val compositeFunc = new DialCodeMetricIndexerFunction(jobConfig)
    val check = compositeFunc.createDialCodeIndex()(mockElasticutil)
    check should be(false)
  }

  "DialCodeMetricIndexerFunction" should "return the event with error message" in {
    val compositeFunc = new DialCodeMetricIndexerFunction(jobConfig)
    val event = getEvent(EventFixture.DATA_NODE_DELETE, 509674)
    val exception = new Exception(s"Test Exception Handling")
    val failedEventString = compositeFunc.getFailedEvent(event, exception)
    failedEventString.isEmpty should be(false)
    failedEventString.contains("failInfo") should be(true)
    failedEventString.contains("jobName") should be(true)
  }

  "Event.index" should "return whether event is indexable " in {
    var eventMap = new util.HashMap[String, Any]()
    eventMap.put("index", "true")
    var event = new Event(eventMap)
    event.index should be(true)

    eventMap.put("index", "false")
    event = new Event(eventMap)
    event.index should be(false)

    eventMap.put("index", null)
    event = new Event(eventMap)
    event.index should be(true)

    eventMap.put("index", true)
    event = new Event(eventMap)
    event.index should be(true)

    eventMap.put("index", false)
    event = new Event(eventMap)
    event.index should be(false)
  }


  def getEvent(event: String, nodeGraphId: Int): Event = {
    val eventMap = ScalaJsonUtil.deserialize[util.Map[String, Any]](event)
    eventMap.put("nodeGraphId", nodeGraphId)
    new Event(eventMap)
  }

}
