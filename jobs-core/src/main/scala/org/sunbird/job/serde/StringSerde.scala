package org.sunbird.job.serde

import java.nio.charset.StandardCharsets

import org.apache.flink.api.common.serialization.SerializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema
import org.apache.flink.streaming.connectors.kafka.{KafkaDeserializationSchema, KafkaSerializationSchema}
import org.apache.flink.util.Collector
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord

class StringDeserializationSchema extends KafkaDeserializationSchema[String] with KafkaRecordDeserializationSchema[String] {

  override def open(context: org.apache.flink.api.common.serialization.DeserializationSchema.InitializationContext): Unit = {
    super[KafkaDeserializationSchema].open(context)
  }

  override def isEndOfStream(nextElement: String): Boolean = false

  override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]]): String = {
    new String(record.value(), StandardCharsets.UTF_8)
  }

  override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]], out: Collector[String]): Unit = {
    out.collect(deserialize(record))
  }

  override def getProducedType: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])
}

class StringSerializationSchema(topic: String, key: Option[String] = None) extends KafkaSerializationSchema[String] with SerializationSchema[String] {

  override def open(context: org.apache.flink.api.common.serialization.SerializationSchema.InitializationContext): Unit = {
    super[KafkaSerializationSchema].open(context)
  }

  override def serialize(element: String, timestamp: java.lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
    key.map { kafkaKey =>
      new ProducerRecord[Array[Byte], Array[Byte]](topic, kafkaKey.getBytes(StandardCharsets.UTF_8), element.getBytes(StandardCharsets.UTF_8))
    }.getOrElse(new ProducerRecord[Array[Byte], Array[Byte]](topic, element.getBytes(StandardCharsets.UTF_8)))
  }

  override def serialize(element: String): Array[Byte] = {
    element.getBytes(StandardCharsets.UTF_8)
  }
}

