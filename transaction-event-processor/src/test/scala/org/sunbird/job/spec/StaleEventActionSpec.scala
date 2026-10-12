package org.sunbird.job.spec

import org.sunbird.job.transaction.compositesearch.helpers.{CompositeSearchIndexerHelper, StaleEventAction}
import org.sunbird.spec.BaseTestSpec

import scala.util.{Failure, Success}

class StaleEventActionSpec extends BaseTestSpec with CompositeSearchIndexerHelper {

  private val node = Success(Some(new java.util.HashMap[String, AnyRef]()))
  private val missing = Success(None)
  private val failed = Failure(new RuntimeException("janus unavailable"))

  "staleEventAction" should "rebuild from the graph for any operation when the node exists" in {
    Seq("CREATE", "UPDATE", "DELETE").foreach { op =>
      staleEventAction(op, node) should be(StaleEventAction.RebuildFromGraph)
    }
  }

  it should "skip a stale CREATE/UPDATE when the node no longer exists so a deleted document is not recreated" in {
    staleEventAction("UPDATE", missing) should be(StaleEventAction.Skip)
    staleEventAction("CREATE", missing) should be(StaleEventAction.Skip)
  }

  it should "still apply a stale DELETE when the node no longer exists" in {
    staleEventAction("DELETE", missing) should be(StaleEventAction.ProcessOriginal)
  }

  it should "fall back to the original event, not drop it, when the graph lookup fails" in {
    Seq("CREATE", "UPDATE", "DELETE").foreach { op =>
      staleEventAction(op, failed) should be(StaleEventAction.ProcessOriginal)
    }
  }
}
