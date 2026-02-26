package org.sunbird.job.serde

import java.nio.charset.StandardCharsets
import java.util

import com.google.gson.Gson
import org.apache.flink.api.common.serialization.SerializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema
import org.apache.flink.streaming.connectors.kafka.{KafkaDeserializationSchema, KafkaSerializationSchema}
import org.apache.flink.util.Collector
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord

import scala.collection.JavaConverters._

class MapDeserializationSchema extends KafkaDeserializationSchema[util.Map[String, AnyRef]] with KafkaRecordDeserializationSchema[util.Map[String, AnyRef]] {

  override def open(context: org.apache.flink.api.common.serialization.DeserializationSchema.InitializationContext): Unit = {
    super[KafkaDeserializationSchema].open(context)
  }

  override def isEndOfStream(nextElement: util.Map[String, AnyRef]): Boolean = false

  override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]]): util.Map[String, AnyRef] = {
    val partition = new Integer(record.partition())
    val parsedString = new String(record.value(), StandardCharsets.UTF_8)
    val recordMap = new Gson().fromJson(parsedString, new util.HashMap[String, AnyRef]().getClass).asScala ++ Map("partition" -> partition.asInstanceOf[AnyRef])
    recordMap.asJava
  }

  override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]], out: Collector[util.Map[String, AnyRef]]): Unit = {
    out.collect(deserialize(record))
  }

  override def getProducedType: TypeInformation[util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])
}

class MapSerializationSchema(topic: String, key: Option[String] = None) extends KafkaSerializationSchema[util.Map[String, AnyRef]] with SerializationSchema[util.Map[String, AnyRef]] {

  override def open(context: org.apache.flink.api.common.serialization.SerializationSchema.InitializationContext): Unit = {
    super[KafkaSerializationSchema].open(context)
  }

  override def serialize(element: util.Map[String, AnyRef], timestamp: java.lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
    val out = new Gson().toJson(element)
    key.map { kafkaKey =>
      new ProducerRecord[Array[Byte], Array[Byte]](topic, kafkaKey.getBytes(StandardCharsets.UTF_8), out.getBytes(StandardCharsets.UTF_8))
    }.getOrElse(new ProducerRecord[Array[Byte], Array[Byte]](topic, out.getBytes(StandardCharsets.UTF_8)))
  }

  override def serialize(element: util.Map[String, AnyRef]): Array[Byte] = {
    new Gson().toJson(element).getBytes(StandardCharsets.UTF_8)
  }
}
