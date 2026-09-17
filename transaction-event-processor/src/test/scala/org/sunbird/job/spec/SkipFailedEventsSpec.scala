package org.sunbird.job.spec

import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.OutputTag
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito
import org.mockito.Mockito.{times, verify}
import org.sunbird.job.Metrics
import org.sunbird.job.exception.InvalidEventException
import org.sunbird.job.fixture.EventFixture
import org.sunbird.job.transaction.domain.Event
import org.sunbird.job.transaction.functions.CompositeSearchIndexerFunction
import org.sunbird.job.transaction.task.TransactionEventProcessorConfig
import org.sunbird.job.util.JSONUtil
import org.sunbird.spec.BaseTestSpec

import java.util

/**
 * An event the indexer cannot process is always written to the error topic. Whether the
 * failure is then rethrown decides if the whole job is cancelled -- and because the job
 * restarts from the same offset, rethrowing means one unindexable document halts
 * indexing for every other document indefinitely.
 */
class SkipFailedEventsSpec extends BaseTestSpec {

  private val baseConfig: Config = ConfigFactory.load("search_indexer_test.conf")

  private def configWith(skip: Option[Boolean]): TransactionEventProcessorConfig = {
    val conf = skip match {
      case Some(v) =>
        baseConfig.withValue("job.skip-failed-events", ConfigValueFactory.fromAnyRef(v))
      case None => baseConfig.withoutPath("job.skip-failed-events")
    }
    new TransactionEventProcessorConfig(conf)
  }

  private def event(): Event = {
    val map = JSONUtil.deserialize[util.Map[String, Any]](EventFixture.EVENT_1)
    new Event(map, 0, 613)
  }

  /**
   * Both collaborators are left null so indexing throws, which is the only path
   * under test here.
   */
  private def failingFunction(config: TransactionEventProcessorConfig) =
    new CompositeSearchIndexerFunction(config, null, null)

  "skipFailedEvents" should "default to false so existing deployments keep failing fast" in {
    configWith(None).skipFailedEvents should be(false)
  }

  it should "be read from configuration when present" in {
    configWith(Some(true)).skipFailedEvents should be(true)
    configWith(Some(false)).skipFailedEvents should be(false)
  }

  "CompositeSearchIndexerFunction" should "rethrow as InvalidEventException when skipping is disabled" in {
    val context = mock[ProcessFunction[Event, String]#Context]
    val metrics = mock[Metrics](Mockito.withSettings().serializable())

    a[InvalidEventException] should be thrownBy {
      failingFunction(configWith(Some(false))).processElement(event(), context, metrics)
    }

    // The event reaches the error topic even on the fail-fast path.
    verify(context, times(1)).output(any[OutputTag[String]](), any[String]())
  }

  it should "write the event to the error topic and carry on when skipping is enabled" in {
    val context = mock[ProcessFunction[Event, String]#Context]
    val metrics = mock[Metrics](Mockito.withSettings().serializable())

    noException should be thrownBy {
      failingFunction(configWith(Some(true))).processElement(event(), context, metrics)
    }

    verify(context, times(1)).output(any[OutputTag[String]](), any[String]())
  }
}
