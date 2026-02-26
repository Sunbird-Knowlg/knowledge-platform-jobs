package org.sunbird.job.serde

import java.nio.charset.StandardCharsets

import com.google.gson.Gson
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema
import org.apache.flink.util.Collector
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.slf4j.LoggerFactory
import org.sunbird.job.domain.reader.JobRequest
import org.sunbird.job.util.JSONUtil

import scala.reflect.{ClassTag, classTag}

class JobRequestDeserializationSchema[T <: JobRequest](implicit ct: ClassTag[T]) extends KafkaRecordDeserializationSchema[T] {

  private[this] val logger = LoggerFactory.getLogger(classOf[JobRequestDeserializationSchema[JobRequest]])

  override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]], out: Collector[T]): Unit = {
    try {
      val result = JSONUtil.deserialize[java.util.HashMap[String, AnyRef]](record.value())
      val args = Array(result, record.partition(), record.offset()).asInstanceOf[Array[AnyRef]]
      out.collect(ct.runtimeClass.getConstructor(classOf[java.util.Map[String, AnyRef]], Integer.TYPE, java.lang.Long.TYPE)
        .newInstance(args: _*).asInstanceOf[T])
    } catch {
      case ex: Exception =>
        ex.printStackTrace()
        logger.error("Exception when parsing event from kafka: " + record, ex)
        val args = Array(new java.util.HashMap[String, AnyRef](), record.partition(), record.offset()).asInstanceOf[Array[AnyRef]]
        out.collect(ct.runtimeClass.getConstructor(classOf[java.util.Map[String, AnyRef]], Integer.TYPE, java.lang.Long.TYPE)
          .newInstance(args: _*).asInstanceOf[T])
    }
  }

  override def getProducedType: TypeInformation[T] = TypeExtractor.getForClass(classTag[T].runtimeClass).asInstanceOf[TypeInformation[T]]
}

class JobRequestSerializationSchema[T <: JobRequest: Manifest](topic: String) extends KafkaRecordSerializationSchema[T] {
  override def serialize(element: T, context: KafkaRecordSerializationSchema.KafkaSinkContext, timestamp: java.lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
    new ProducerRecord[Array[Byte], Array[Byte]](topic, Option(element.kafkaKey()).map(_.getBytes(StandardCharsets.UTF_8)).orNull,
      element.getJson().getBytes(StandardCharsets.UTF_8))
  }
}
