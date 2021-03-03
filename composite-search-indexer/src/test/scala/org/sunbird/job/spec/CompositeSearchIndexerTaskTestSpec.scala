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
import org.mockito.Mockito.{doNothing, when}
import org.sunbird.job.compositesearch.domain.Event
import org.sunbird.job.connector.FlinkKafkaConnector
import org.sunbird.job.fixture.EventFixture
import org.sunbird.job.functions.{CompositeSearchIndexerFunction, DialCodeMetricIndexerFunction}
import org.sunbird.job.task.CompositeSearchIndexerConfig
import org.sunbird.job.util.{ElasticSearchUtil, ScalaJsonUtil}
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
    // TODO
    flinkCluster.before()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    try {
      // TODO
    } catch {
      case ex: Exception => {
      }
    }
    flinkCluster.after()
  }

  "getCompositeIndexerobject " should " return Composite Object for the event" in {
    val event = getCreateEventForCompositeIndexer(EventFixture.DATA_NODE_CREATE, 509674)
    val compositeObject = new CompositeSearchIndexerFunction(jobConfig).getCompositeIndexerobject(event)
    compositeObject.objectType should be("Collection")
    compositeObject.getVersionAsString() should be("1.0")
    compositeObject.uniqueId should be("do_1132247274257203201191")
  }

  "processESMessage " should " index the event for the appropriate fields" in {
    doNothing().when(mockElasticutil).addDocumentWithId(anyString(), anyString())

    val event = getCreateEventForCompositeIndexer(EventFixture.DATA_NODE_CREATE, 509674)
    val compositeFunc = new CompositeSearchIndexerFunction(jobConfig)
    val compositeObject = compositeFunc.getCompositeIndexerobject(event)
    compositeFunc.processESMessage(compositeObject)(mockElasticutil)
  }

  "processESMessage " should " update the indexed event for the new values of the properties" in {
    doNothing().when(mockElasticutil).addDocumentWithId(anyString(), anyString())
    val documentJson = "{\"ownershipType\":[\"createdBy\"],\"code\":\"org.sunbird.zf7fcK\",\"credentials\":{\"enabled\":\"No\"},\"subject\":[\"Geography\"],\"channel\":\"channel-1\",\"language\":[\"English\"],\"mimeType\":\"application/vnd.ekstep.content-collection\",\"idealScreenSize\":\"normal\",\"createdOn\":\"2021-02-26T13:36:49.592+0000\",\"objectType\":\"Collection\",\"primaryCategory\":\"Digital Textbook\",\"contentDisposition\":\"inline\",\"additionalCategories\":[\"Textbook\"],\"lastUpdatedOn\":\"2021-02-26T13:36:49.592+0000\",\"contentEncoding\":\"gzip\",\"dialcodeRequired\":\"No\",\"contentType\":\"TextBook\",\"trackable\":{\"enabled\":\"No\",\"autoBatch\":\"No\"},\"identifier\":\"do_1132247274257203201191\",\"subjectIds\":[\"ncf_subject_geography\"],\"lastStatusChangedOn\":\"2021-02-26T13:36:49.592+0000\",\"audience\":[\"Student\"],\"IL_SYS_NODE_TYPE\":\"DATA_NODE\",\"os\":[\"All\"],\"visibility\":\"Default\",\"consumerId\":\"7411b6bd-89f3-40ec-98d1-229dc64ce77d\",\"mediaType\":\"content\",\"osId\":\"org.ekstep.quiz.app\",\"graph_id\":\"domain\",\"nodeType\":\"DATA_NODE\",\"version\":2,\"versionKey\":\"1614346609592\",\"idealScreenDensity\":\"hdpi\",\"license\":\"CC BY-SA 4.0\",\"framework\":\"NCF\",\"createdBy\":\"95e4942d-cbe8-477d-aebd-ad8e6de4bfc8\",\"compatibilityLevel\":1,\"IL_FUNC_OBJECT_TYPE\":\"Collection\",\"userConsent\":\"Yes\",\"name\":\"Test\",\"IL_UNIQUE_ID\":\"do_1132247274257203201191\",\"status\":\"Draft\",\"node_id\":509674}"
    when(mockElasticutil.getDocumentAsStringById(anyString())).thenReturn(documentJson)

    val event = getCreateEventForCompositeIndexer(EventFixture.DATA_NODE_UPDATE, 509674)
    val compositeFunc = new CompositeSearchIndexerFunction(jobConfig)
    val compositeObject = compositeFunc.getCompositeIndexerobject(event)
    compositeFunc.processESMessage(compositeObject)(mockElasticutil)
  }

  "processESMessage " should " delete the indexed event " in {
    doNothing().when(mockElasticutil).deleteDocument(anyString())
    val documentJson = "{\"ownershipType\":[\"createdBy\"],\"code\":\"org.sunbird.zf7fcK\",\"credentials\":{\"enabled\":\"No\"},\"subject\":[\"Geography\"],\"channel\":\"channel-1\",\"language\":[\"English\"],\"mimeType\":\"application/vnd.ekstep.content-collection\",\"idealScreenSize\":\"normal\",\"createdOn\":\"2021-02-26T13:36:49.592+0000\",\"objectType\":\"Collection\",\"primaryCategory\":\"Digital Textbook\",\"contentDisposition\":\"inline\",\"additionalCategories\":[\"Textbook\"],\"lastUpdatedOn\":\"2021-02-26T13:36:49.592+0000\",\"contentEncoding\":\"gzip\",\"dialcodeRequired\":\"No\",\"contentType\":\"TextBook\",\"trackable\":{\"enabled\":\"No\",\"autoBatch\":\"No\"},\"identifier\":\"do_1132247274257203201191\",\"subjectIds\":[\"ncf_subject_geography\"],\"lastStatusChangedOn\":\"2021-02-26T13:36:49.592+0000\",\"audience\":[\"Student\"],\"IL_SYS_NODE_TYPE\":\"DATA_NODE\",\"os\":[\"All\"],\"visibility\":\"Default\",\"consumerId\":\"7411b6bd-89f3-40ec-98d1-229dc64ce77d\",\"mediaType\":\"content\",\"osId\":\"org.ekstep.quiz.app\",\"graph_id\":\"domain\",\"nodeType\":\"DATA_NODE\",\"version\":2,\"versionKey\":\"1614346609592\",\"idealScreenDensity\":\"hdpi\",\"license\":\"CC BY-SA 4.0\",\"framework\":\"NCF\",\"createdBy\":\"95e4942d-cbe8-477d-aebd-ad8e6de4bfc8\",\"compatibilityLevel\":1,\"IL_FUNC_OBJECT_TYPE\":\"Collection\",\"userConsent\":\"Yes\",\"name\":\"Test\",\"IL_UNIQUE_ID\":\"do_1132247274257203201191\",\"status\":\"Draft\",\"node_id\":509674}"
    when(mockElasticutil.getDocumentAsStringById(anyString())).thenReturn(documentJson)

    val event = getCreateEventForCompositeIndexer(EventFixture.DATA_NODE_DELETE, 509674)
    val compositeFunc = new CompositeSearchIndexerFunction(jobConfig)
    val compositeObject = compositeFunc.getCompositeIndexerobject(event)
    compositeFunc.processESMessage(compositeObject)(mockElasticutil)
  }

  "processESMessage " should " not delete the indexed event with visibility Parent " in {
    val documentJson = "{\"ownershipType\":[\"createdBy\"],\"code\":\"org.sunbird.zf7fcK\",\"credentials\":{\"enabled\":\"No\"},\"subject\":[\"Geography\"],\"channel\":\"channel-1\",\"language\":[\"English\"],\"mimeType\":\"application/vnd.ekstep.content-collection\",\"idealScreenSize\":\"normal\",\"createdOn\":\"2021-02-26T13:36:49.592+0000\",\"objectType\":\"Collection\",\"primaryCategory\":\"Digital Textbook\",\"contentDisposition\":\"inline\",\"additionalCategories\":[\"Textbook\"],\"lastUpdatedOn\":\"2021-02-26T13:36:49.592+0000\",\"contentEncoding\":\"gzip\",\"dialcodeRequired\":\"No\",\"contentType\":\"TextBook\",\"trackable\":{\"enabled\":\"No\",\"autoBatch\":\"No\"},\"identifier\":\"do_1132247274257203201191\",\"subjectIds\":[\"ncf_subject_geography\"],\"lastStatusChangedOn\":\"2021-02-26T13:36:49.592+0000\",\"audience\":[\"Student\"],\"IL_SYS_NODE_TYPE\":\"DATA_NODE\",\"os\":[\"All\"],\"visibility\":\"Parent\",\"consumerId\":\"7411b6bd-89f3-40ec-98d1-229dc64ce77d\",\"mediaType\":\"content\",\"osId\":\"org.ekstep.quiz.app\",\"graph_id\":\"domain\",\"nodeType\":\"DATA_NODE\",\"version\":2,\"versionKey\":\"1614346609592\",\"idealScreenDensity\":\"hdpi\",\"license\":\"CC BY-SA 4.0\",\"framework\":\"NCF\",\"createdBy\":\"95e4942d-cbe8-477d-aebd-ad8e6de4bfc8\",\"compatibilityLevel\":1,\"IL_FUNC_OBJECT_TYPE\":\"Collection\",\"userConsent\":\"Yes\",\"name\":\"Test\",\"IL_UNIQUE_ID\":\"do_1132247274257203201191\",\"status\":\"Draft\",\"node_id\":509674}"
    when(mockElasticutil.getDocumentAsStringById(anyString())).thenReturn(documentJson)

    val event = getCreateEventForCompositeIndexer(EventFixture.DATA_NODE_DELETE, 509674)
    val compositeFunc = new CompositeSearchIndexerFunction(jobConfig)
    val compositeObject = compositeFunc.getCompositeIndexerobject(event)
    compositeFunc.processESMessage(compositeObject)(mockElasticutil)
  }

  "processESMessage " should " index the event with the added Relations" in {
    doNothing().when(mockElasticutil).addDocumentWithId(anyString(), anyString())

    val event = getCreateEventForCompositeIndexer(EventFixture.DATA_NODE_CREATE_WITH_RELATION, 509674)
    val compositeFunc = new CompositeSearchIndexerFunction(jobConfig)
    val compositeObject = compositeFunc.getCompositeIndexerobject(event)
    compositeFunc.processESMessage(compositeObject)(mockElasticutil)
  }

  "upsertDialcodeMetricDocument " should " index the dialcode metrics " in {
    doNothing().when(mockElasticutil).addDocumentWithId(anyString(), anyString())
    when(mockElasticutil.getDocumentAsStringById(anyString())).thenReturn("")

    val event = getCreateEventForCompositeIndexer(EventFixture.DATA_NODE_CREATE_WITH_RELATION, 509674)
    val dialcodeMetricFunc = new DialCodeMetricIndexerFunction(jobConfig)
    val uniqueId = event.readOrDefault("nodeUniqueId", "")
    dialcodeMetricFunc.upsertDialcodeMetricDocument(uniqueId, event.getMap().asScala.toMap)(mockElasticutil)
  }

  def getCreateEventForCompositeIndexer(event: String, nodeGraphId: Int): Event = {
    val eventMap = ScalaJsonUtil.deserialize[util.Map[String, Any]](event)
    eventMap.put("nodeGraphId", nodeGraphId)
    new Event(eventMap)
  }

}
