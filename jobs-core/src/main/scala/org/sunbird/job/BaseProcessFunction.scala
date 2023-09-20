package org.sunbird.job

import org.apache.flink.api.scala.metrics.ScalaGauge
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.{GlobalWindow, TimeWindow}
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

case class Metrics(metrics: ConcurrentHashMap[String, AtomicLong]) {
  private[this] lazy val logger = LoggerFactory.getLogger(classOf[Metrics])
  def incCounter(metric: String): Unit = {
    logger.info("Metrics: " + metrics)
    logger.info("Metric key: " + metric)
    logger.info("Metric value: " + metrics.get(metric))
    metrics.get(metric).getAndIncrement()
  }

  def getAndReset(metric: String): Long = metrics.get(metric).getAndSet(0L)

  def get(metric: String): Long = metrics.get(metric).get()

  def reset(metric: String): Unit = metrics.get(metric).set(0L)
}

trait JobMetrics {
  private[this] lazy val logger = LoggerFactory.getLogger(classOf[JobMetrics])
  def registerMetrics(metrics: List[String]): Metrics = {
    val metricMap = new ConcurrentHashMap[String, AtomicLong]()
    metrics.map { metric => metricMap.put(metric, new AtomicLong(0L)) }
    Metrics(metricMap)
  }
}

abstract class BaseProcessFunction[T, R](config: BaseJobConfig) extends ProcessFunction[T, R] with JobMetrics {

  private[this] lazy val logger = LoggerFactory.getLogger(classOf[JobMetrics])

  private val metrics: Metrics = registerMetrics(metricsList())

  override def open(parameters: Configuration): Unit = {
    metricsList().map { metric =>
      getRuntimeContext.getMetricGroup.addGroup(config.jobName)
        .gauge[Long, ScalaGauge[Long]](metric, ScalaGauge[Long](() => metrics.getAndReset(metric)))
    }
  }

  def processElement(event: T, context: ProcessFunction[T, R]#Context, metrics: Metrics): Unit

  def metricsList(): List[String]

  override def processElement(event: T, context: ProcessFunction[T, R]#Context, out: Collector[R]): Unit = {
    processElement(event, context, metrics)
  }
}

abstract class WindowBaseProcessFunction[I, O, K](config: BaseJobConfig) extends ProcessWindowFunction[I, O, K, GlobalWindow] with JobMetrics {

  private val metrics: Metrics = registerMetrics(metricsList())

  override def open(parameters: Configuration): Unit = {
    metricsList().map { metric =>
      getRuntimeContext.getMetricGroup.addGroup(config.jobName)
        .gauge[Long, ScalaGauge[Long]](metric, ScalaGauge[Long](() => metrics.getAndReset(metric)))
    }
  }

  def metricsList(): List[String]

  def process(key: K,
              context: ProcessWindowFunction[I, O, K, GlobalWindow]#Context,
              elements: Iterable[I],
              metrics: Metrics): Unit

  override def process(key: K, context: Context, elements: Iterable[I], out: Collector[O]): Unit = {
    process(key, context, elements, metrics)
  }
}

abstract class TimeWindowBaseProcessFunction[I, O, K](config: BaseJobConfig) extends ProcessWindowFunction[I, O, K, TimeWindow] with JobMetrics {
  private val metrics: Metrics = registerMetrics(metricsList())

  override def open(parameters: Configuration): Unit = {
    metricsList().map { metric =>
      getRuntimeContext.getMetricGroup.addGroup(config.jobName)
        .gauge[Long, ScalaGauge[Long]](metric, ScalaGauge[Long](() => metrics.getAndReset(metric)))
    }
  }

  def metricsList(): List[String]

  def process(key: K,
              context: ProcessWindowFunction[I, O, K, TimeWindow]#Context,
              elements: Iterable[I],
              metrics: Metrics): Unit

  override def process(key: K, context: Context, elements: Iterable[I], out: Collector[O]): Unit = {
    process(key, context, elements, metrics)
  }
}

abstract class BaseProcessKeyedFunction[K, T, R](config: BaseJobConfig) extends KeyedProcessFunction[K, T, R] with JobMetrics {

  private val metrics: Metrics = registerMetrics(metricsList())

  override def open(parameters: Configuration): Unit = {
    metricsList().map { metric =>
      getRuntimeContext.getMetricGroup.addGroup(config.jobName)
        .gauge[Long, ScalaGauge[Long]](metric, ScalaGauge[Long](() => metrics.getAndReset(metric)))
    }
    open(parameters, metrics)
  }

  def open(parameters: Configuration, metrics: Metrics): Unit = {}

  def processElement(event: T, context: KeyedProcessFunction[K, T, R]#Context, metrics: Metrics): Unit

  def onTimer(timestamp: Long, ctx: KeyedProcessFunction[K, T, R]#OnTimerContext, metrics: Metrics): Unit = {}

  def metricsList(): List[String]

  override def processElement(event: T, context: KeyedProcessFunction[K, T, R]#Context, out: Collector[R]): Unit = {
    processElement(event, context, metrics)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[K, T, R]#OnTimerContext, out: Collector[R]): Unit = {
    onTimer(timestamp, ctx, metrics)
  }

}