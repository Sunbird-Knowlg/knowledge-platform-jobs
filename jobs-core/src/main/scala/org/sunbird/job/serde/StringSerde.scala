package org.sunbird.job.serde

import java.nio.charset.StandardCharsets

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.connectors.kafka.{KafkaDeserializationSchema, KafkaSerializationSchema}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema
import org.apache.flink.util.Collector


/*class StringDeserializationSchema extends KafkaDeserializationSchema[String] {

  override def isEndOfStream(nextElement: String): Boolean = false

  override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]]): String = {
    new String(record.value(), StandardCharsets.UTF_8)
  }

  override def getProducedType: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])
}

class StringSerializationSchema(topic: String, key: Option[String] = None) extends KafkaSerializationSchema[String] {

  override def serialize(element: String, timestamp: java.lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
    key.map { kafkaKey =>
      new ProducerRecord[Array[Byte], Array[Byte]](topic, kafkaKey.getBytes(StandardCharsets.UTF_8), element.getBytes(StandardCharsets.UTF_8))
    }.getOrElse(new ProducerRecord[Array[Byte], Array[Byte]](topic, element.getBytes(StandardCharsets.UTF_8)))
  }
}*/

class StringDeserializationSchema extends KafkaRecordDeserializationSchema[String] {

  override def getProducedType: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

  override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]], out: Collector[String]): Unit = {
    out.collect(new String(record.value(), StandardCharsets.UTF_8))
  }
}

class StringSerializationSchema(topic: String, key: Option[String] = None) extends KafkaRecordSerializationSchema[String] {

  override def serialize(element: String, context: KafkaRecordSerializationSchema.KafkaSinkContext, timestamp: java.lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
    key.map { kafkaKey =>
      new ProducerRecord[Array[Byte], Array[Byte]](topic, kafkaKey.getBytes(StandardCharsets.UTF_8), element.getBytes(StandardCharsets.UTF_8))
    }.getOrElse(new ProducerRecord[Array[Byte], Array[Byte]](topic, element.getBytes(StandardCharsets.UTF_8)))
  }
}




