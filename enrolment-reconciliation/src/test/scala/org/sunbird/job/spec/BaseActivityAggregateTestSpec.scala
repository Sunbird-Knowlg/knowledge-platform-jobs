package org.sunbird.job.spec

import org.apache.flink.streaming.api.functions.sink.SinkFunction

import java.util


class AuditEventSink extends SinkFunction[String] {

  override def invoke(value: String): Unit = {
    synchronized {
      AuditEventSink.values.add(value)
    }
  }
}

object AuditEventSink {
  val values: util.List[String] = new util.ArrayList()
}

class FailedEventSink extends SinkFunction[String] {

  override def invoke(value: String): Unit = {
    synchronized {
      FailedEventSink.values.add(value)
    }
  }
}

object FailedEventSink {
  val values: util.List[String] = new util.ArrayList()
}

class SuccessEvent extends SinkFunction[String] {

  override def invoke(value: String): Unit = {
    synchronized {
      SuccessEventSink.values.add(value)
    }
  }
}

object SuccessEventSink {
  val values: util.List[String] = new util.ArrayList()
}


class CertificateIssuedEventsSink extends SinkFunction[String] {

  override def invoke(value: String): Unit = {
    synchronized {
      CertificateIssuedEvents.values.add(value)
    }
  }
}

object CertificateIssuedEvents {
  val values: util.List[String] = new util.ArrayList()
}
