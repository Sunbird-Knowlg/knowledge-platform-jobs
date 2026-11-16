package org.sunbird.job.spec

import com.typesafe.config.ConfigFactory
import org.mockito.ArgumentMatchers.anyString
import org.mockito.Mockito.{never, verify, when}
import org.sunbird.job.domain.`object`.{DefinitionCache, ObjectDefinition}
import org.sunbird.job.fixture.EventFixture
import org.sunbird.job.transaction.compositesearch.helpers.{CompositeSearchIndexerHelper, StaleEventAction, VersionCheck}
import org.sunbird.job.transaction.domain.Event
import org.sunbird.job.transaction.task.TransactionEventProcessorConfig
import org.sunbird.job.util.{ElasticSearchUtil, ScalaJsonUtil}
import org.sunbird.spec.BaseTestSpec

import java.util
import scala.collection.mutable
import scala.util.{Failure, Success}

class IndexedVersionCheckSpec extends BaseTestSpec with CompositeSearchIndexerHelper {

  private val jobConfig = new TransactionEventProcessorConfig(ConfigFactory.load("search_indexer_test.conf"))
  // the real DefinitionCache fetches the object schema from a base path that is not available to unit tests
  private val defCache = mock[DefinitionCache]
  when(defCache.getDefinition(anyString(), anyString(), anyString()))
    .thenReturn(new ObjectDefinition("Collection", "1.0", Map[String, AnyRef](), Map[String, AnyRef]()))

  // epoch millis for the timestamps used below
  private def ts(s: String): Long =
    new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ").parse(s).getTime
  private val T1 = ts("2026-10-12T06:31:05.231+0000")
  private val T2 = ts("2026-10-12T06:31:05.394+0000")
  private val T3 = ts("2026-10-12T06:31:30.564+0000")

  private def doc(lastUpdatedOn: String): Option[Map[String, AnyRef]] =
    Some(Map[String, AnyRef]("lastUpdatedOn" -> lastUpdatedOn))
  private val indexedAtT2 = doc("2026-10-12T06:31:05.394+0000")

  private def event(operationType: String): Event = {
    val eventMap = ScalaJsonUtil.deserialize[util.Map[String, Any]](EventFixture.DATA_NODE_UPDATE)
    eventMap.put("nodeGraphId", 509674)
    eventMap.put("operationType", operationType)
    new Event(eventMap, 0, 15)
  }

  "cacheCheck" should "judge an event against the cached timestamp on a hit" in {
    cacheCheck("UPDATE", Some(T3), Some(T2)) should be(Some(VersionCheck.Apply))
    cacheCheck("UPDATE", Some(T2), Some(T2)) should be(Some(VersionCheck.CheckGraph))
    cacheCheck("UPDATE", Some(T1), Some(T2)) should be(Some(VersionCheck.CheckGraph))
    cacheCheck("DELETE", Some(T1), Some(T2)) should be(Some(VersionCheck.CheckGraph))
  }

  it should "apply an event whose timestamp is unknown on a hit" in {
    cacheCheck("UPDATE", None, Some(T2)) should be(Some(VersionCheck.Apply))
  }

  it should "apply a CREATE on a miss without reading the indexed document" in {
    cacheCheck("CREATE", Some(T1), None) should be(Some(VersionCheck.Apply))
  }

  it should "leave UPDATE and DELETE on a miss to be judged against the indexed document" in {
    cacheCheck("UPDATE", Some(T1), None) should be(None)
    cacheCheck("DELETE", Some(T1), None) should be(None)
  }

  "versionCheck for CREATE" should "always apply the event, whatever is indexed" in {
    versionCheck("CREATE", Some(T1), None) should be(VersionCheck.Apply)
    versionCheck("CREATE", Some(T1), indexedAtT2) should be(VersionCheck.Apply)
  }

  "versionCheck for UPDATE" should "apply an event newer than the indexed document" in {
    versionCheck("UPDATE", Some(T3), indexedAtT2) should be(VersionCheck.Apply)
  }

  it should "check JanusGraph for an older or equal event" in {
    versionCheck("UPDATE", Some(T1), indexedAtT2) should be(VersionCheck.CheckGraph)
    versionCheck("UPDATE", Some(T2), indexedAtT2) should be(VersionCheck.CheckGraph)
  }

  it should "check JanusGraph when there is no indexed document to apply to" in {
    versionCheck("UPDATE", Some(T2), None) should be(VersionCheck.CheckGraph)
  }

  it should "apply the event when the timestamps cannot be compared" in {
    versionCheck("UPDATE", None, indexedAtT2) should be(VersionCheck.Apply)
    versionCheck("UPDATE", Some(T1), Some(Map[String, AnyRef]("name" -> "x"))) should be(VersionCheck.Apply)
  }

  "versionCheck for DELETE" should "apply a DELETE for the version that is indexed, including an equal timestamp" in {
    versionCheck("DELETE", Some(T2), indexedAtT2) should be(VersionCheck.Apply)
    versionCheck("DELETE", Some(T3), indexedAtT2) should be(VersionCheck.Apply)
  }

  it should "skip a DELETE older than the indexed document, which belongs to a newer incarnation of the node" in {
    versionCheck("DELETE", Some(T1), indexedAtT2) should be(VersionCheck.Skip)
  }

  it should "skip a DELETE when nothing is indexed" in {
    versionCheck("DELETE", Some(T2), None) should be(VersionCheck.Skip)
  }

  "versionCheck" should "apply events of an unknown operation so that they are logged and ignored downstream" in {
    versionCheck("UNKNOWN", Some(T1), indexedAtT2) should be(VersionCheck.Apply)
  }

  "captured sequences" should "skip the UPDATE that arrives after its node was deleted, even after a restart" in {
    // cache miss after a restart, no document, node gone from JanusGraph
    cacheCheck("UPDATE", Some(T3), None) should be(None)
    versionCheck("UPDATE", Some(T3), None) should be(VersionCheck.CheckGraph)
    staleEventAction("UPDATE", Success(None)) should be(StaleEventAction.Skip)
  }

  it should "rebuild from the graph an UPDATE older than the CREATE snapshot already indexed" in {
    // CREATE 14747 indexed the state as of 05.394; UPDATE 14748 (05.231) arrives afterwards
    versionCheck("UPDATE", Some(T1), indexedAtT2) should be(VersionCheck.CheckGraph)
    staleEventAction("UPDATE", Success(Some(new util.HashMap[String, AnyRef]()))) should be(StaleEventAction.RebuildFromGraph)
  }

  it should "not let a late DELETE of an earlier cycle remove the document of a newer cycle" in {
    versionCheck("DELETE", Some(T1), indexedAtT2) should be(VersionCheck.Skip)
  }

  it should "index the event as received when JanusGraph cannot be reached" in {
    versionCheck("UPDATE", Some(T1), indexedAtT2) should be(VersionCheck.CheckGraph)
    staleEventAction("UPDATE", Failure(new RuntimeException("janus unavailable"))) should be(StaleEventAction.ProcessOriginal)
  }

  "readIndexedDocument" should "return None when nothing is indexed and the document when it is" in {
    val es = mock[ElasticSearchUtil]
    when(es.getDocumentAsString("absent")).thenReturn(null)
    when(es.getDocumentAsString("present"))
      .thenReturn("""{"identifier":"present","lastUpdatedOn":"2026-10-12T06:31:05.394+0000"}""")
    readIndexedDocument("absent")(es) should be(None)
    readIndexedDocument("present")(es).map(d => indexedLastUpdatedOn(d)) should be(Some(Some(T2)))
  }

  "processESMessage" should "use the document it is given instead of reading it again" in {
    val es = mock[ElasticSearchUtil]
    val existing = mutable.Map[String, AnyRef]("identifier" -> "do_1132247274257203201191", "visibility" -> "Default")
    processESMessage(getCompositeIndexerObject(event("DELETE"))(jobConfig), Some(existing))(es, defCache)
    verify(es, never()).getDocumentAsString(anyString())
    verify(es).deleteDocument("do_1132247274257203201191")
  }

  it should "still keep a Parent document when deleting" in {
    val es = mock[ElasticSearchUtil]
    val existing = mutable.Map[String, AnyRef]("identifier" -> "do_1132247274257203201191", "visibility" -> "Parent")
    processESMessage(getCompositeIndexerObject(event("DELETE"))(jobConfig), Some(existing))(es, defCache)
    verify(es, never()).deleteDocument(anyString())
  }
}
