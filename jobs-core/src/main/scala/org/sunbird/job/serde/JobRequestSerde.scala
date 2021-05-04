package org.sunbird.job.serde

import java.nio.charset.StandardCharsets

import com.google.gson.Gson
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.connectors.kafka.{KafkaDeserializationSchema, KafkaSerializationSchema}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.slf4j.LoggerFactory
import org.sunbird.job.domain.reader.JobRequest
import org.sunbird.job.util.JSONUtil

import scala.reflect.{ClassTag, classTag}

class JobRequestDeserializationSchema[T <: JobRequest](implicit ct: ClassTag[T]) extends KafkaDeserializationSchema[T]  {

  override def isEndOfStream(nextElement: T): Boolean = false
  private[this] val logger = LoggerFactory.getLogger(classOf[JobRequestDeserializationSchema[JobRequest]])

  override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]]): T = {
    try {
      val result = JSONUtil.deserialize[java.util.HashMap[String, AnyRef]](record.value())
      val args = Array(result, record.partition(), record.offset()).asInstanceOf[Array[AnyRef]]
      ct.runtimeClass.getConstructor(classOf[java.util.Map[String, AnyRef]], Integer.TYPE, java.lang.Long.TYPE)
        .newInstance(args:_*).asInstanceOf[T]
    } catch {
      case ex: Exception =>
        ex.printStackTrace()
        logger.error("Exception when parsing event from kafka: " + record, ex)
        val args = Array(new java.util.HashMap[String, AnyRef](), record.partition(), record.offset()).asInstanceOf[Array[AnyRef]]
        ct.runtimeClass.getConstructor(classOf[java.util.Map[String, AnyRef]], Integer.TYPE, java.lang.Long.TYPE)
          .newInstance(args:_*).asInstanceOf[T]
    }
  }

  override def getProducedType: TypeInformation[T] = TypeExtractor.getForClass(classTag[T].runtimeClass).asInstanceOf[TypeInformation[T]]
}

class JobRequestSerializationSchema[T <: JobRequest: Manifest](topic: String) extends KafkaSerializationSchema[T] {
  override def serialize(element: T, timestamp: java.lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
    new ProducerRecord[Array[Byte], Array[Byte]](topic, Option(element.kafkaKey()).map(_.getBytes(StandardCharsets.UTF_8)).orNull,
      element.getJson().getBytes(StandardCharsets.UTF_8))
  }
}
