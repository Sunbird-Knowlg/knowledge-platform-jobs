package org.sunbird.job.contentembedding.function

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.contentembedding.task.ContentEmbeddingConfig
import org.sunbird.job.{BaseProcessKeyedFunction, Metrics}

import scala.collection.JavaConverters._

/**
 * Shared windowing protocol for batch-accumulating [[KeyedProcessFunction]]s.
 *
 * Manages a [[ListState]] buffer, a count [[ValueState]] (O(1) per element),
 * and a processing-time timer. Flush is triggered by whichever comes first:
 *  - Buffer reaches [[batchSize]] events.
 *  - Timer fires after [[windowMs]] from the first event in the buffer.
 *
 * Subclasses implement [[doFlush]] for the actual work and call
 * [[initWindowState]] inside their `open()` with the correct [[ListStateDescriptor]].
 */
abstract class BaseBatchingKeyedFunction[K, I, O](config: ContentEmbeddingConfig)
    extends BaseProcessKeyedFunction[K, I, O](config) {

  private[this] val logger = LoggerFactory.getLogger(getClass)

  protected def batchSize: Int
  protected def windowMs: Long

  protected def doFlush(
      events: List[I],
      context: KeyedProcessFunction[K, I, O]#Context,
      metrics: Metrics
  ): Unit

  @transient private var bufferState: ListState[I] = _
  @transient private var bufferCount: ValueState[java.lang.Integer] = _
  @transient private var pendingTimer: ValueState[java.lang.Long] = _

  protected def initWindowState(descriptor: ListStateDescriptor[I], stateKeyPrefix: String): Unit = {
    bufferState = getRuntimeContext.getListState(descriptor)
    bufferCount = getRuntimeContext.getState(
      new ValueStateDescriptor[java.lang.Integer](s"$stateKeyPrefix-count", Types.INT)
    )
    pendingTimer = getRuntimeContext.getState(
      new ValueStateDescriptor[java.lang.Long](s"$stateKeyPrefix-timer", Types.LONG)
    )
  }

  override def processElement(
      event: I,
      context: KeyedProcessFunction[K, I, O]#Context,
      metrics: Metrics
  ): Unit = {
    val count = Option(bufferCount.value()).map(_.intValue()).getOrElse(0)

    bufferState.add(event)
    bufferCount.update(count + 1)

    if (count == 0 && windowMs > 0) {
      val flushAt = context.timerService().currentProcessingTime() + windowMs
      context.timerService().registerProcessingTimeTimer(flushAt)
      pendingTimer.update(flushAt)
    }

    if (count + 1 >= batchSize) {
      logger.debug(s"Batch size threshold reached (${count + 1}), flushing")
      cancelPendingTimer(context.timerService())
      flushBuffer(context, metrics)
    }
  }

  override def onTimer(
      timestamp: Long,
      ctx: KeyedProcessFunction[K, I, O]#OnTimerContext,
      metrics: Metrics
  ): Unit = {
    val registered = pendingTimer.value()
    if (registered != null && registered == timestamp) {
      val count = Option(bufferCount.value()).map(_.intValue()).getOrElse(0)
      if (count > 0) {
        logger.debug(s"Window timer fired, flushing $count buffered events")
        pendingTimer.clear()
        flushBuffer(ctx, metrics)
      }
    }
  }

  private def cancelPendingTimer(timerService: org.apache.flink.streaming.api.TimerService): Unit = {
    val ts = pendingTimer.value()
    if (ts != null) {
      timerService.deleteProcessingTimeTimer(ts)
      pendingTimer.clear()
    }
  }

  private def flushBuffer(
      context: KeyedProcessFunction[K, I, O]#Context,
      metrics: Metrics
  ): Unit = {
    val events = bufferState.get().asScala.toList
    bufferState.clear()
    bufferCount.clear()
    if (events.nonEmpty) doFlush(events, context, metrics)
  }
}
