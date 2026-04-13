package org.sunbird.spec

import org.apache.flink.metrics.{Gauge, Metric, MetricConfig, MetricGroup}
import org.apache.flink.metrics.reporter.MetricReporter

import scala.collection.mutable

class BaseMetricsReporter extends MetricReporter {

  override def open(config: MetricConfig): Unit = {}

  override def close(): Unit = {}

  override def notifyOfAddedMetric(metric: Metric, metricName: String, group: MetricGroup): Unit = {
    metric match {
      case gauge: Gauge[_] => {
        val gaugeKey = group.getScopeComponents.toSeq.drop(6).mkString(".") + "." + metricName
        BaseMetricsReporter.gaugeMetrics(gaugeKey) = gauge.asInstanceOf[Gauge[Long]]
      }
      case _ => // Do Nothing
    }
  }
  override def notifyOfRemovedMetric(metric: Metric, metricName: String, group: MetricGroup): Unit = {}
}

object BaseMetricsReporter {
  val gaugeMetrics: mutable.Map[String, Gauge[Long]] = mutable.Map[String, Gauge[Long]]()
}
