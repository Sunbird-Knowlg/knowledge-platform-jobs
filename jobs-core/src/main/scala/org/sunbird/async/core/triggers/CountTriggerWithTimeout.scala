package org.sunbird.async.core.triggers

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.state.{ReducingState, ReducingStateDescriptor}
import org.apache.flink.api.common.typeutils.base.LongSerializer
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.windowing.triggers.Trigger.TriggerContext
import org.apache.flink.streaming.api.windowing.triggers._
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

/**
 * A trigger that fires when the count of elements in a pane reaches the given count or a
 * timeout is reached whatever happens first.
 */
class CountTriggerWithTimeout[W <: TimeWindow](maxCount: Long, timeCharacteristic: TimeCharacteristic) extends Trigger[Object, W] {
  private val countState: ReducingStateDescriptor[java.lang.Long] = new ReducingStateDescriptor[java.lang.Long]("count", new Sum(), LongSerializer.INSTANCE)

  override def onElement(element: Object, timestamp: Long, window: W, ctx: TriggerContext): TriggerResult = {
    val count: ReducingState[java.lang.Long] = ctx.getPartitionedState(countState)
    count.add(1L)
    if (count.get >= maxCount || timestamp >= window.getEnd) TriggerResult.FIRE_AND_PURGE else TriggerResult.CONTINUE
  }

  override def onProcessingTime(time: Long, window: W, ctx: TriggerContext): TriggerResult = {
    if (timeCharacteristic == TimeCharacteristic.EventTime) TriggerResult.CONTINUE
    else {
      if (time >= window.getEnd) TriggerResult.CONTINUE else TriggerResult.FIRE_AND_PURGE
    }
  }

  override def onEventTime(time: Long, window: W, ctx: TriggerContext): TriggerResult = {
    if (timeCharacteristic == TimeCharacteristic.ProcessingTime) TriggerResult.CONTINUE else {
      if (time >= window.getEnd) TriggerResult.CONTINUE else TriggerResult.FIRE_AND_PURGE
    }
  }

  override def clear(window: W, ctx: TriggerContext): Unit = {
    ctx.getPartitionedState(countState).clear
  }

  class Sum extends ReduceFunction[java.lang.Long] {
    def reduce(value1: java.lang.Long, value2: java.lang.Long): java.lang.Long = value1 + value2
  }

}
