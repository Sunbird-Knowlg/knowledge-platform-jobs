package org.sunbird.job.serde

import java.nio.charset.StandardCharsets
import java.util

import com.google.gson.Gson
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema
import org.apache.flink.util.Collector
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord

import scala.collection.JavaConverters._

class MapDeserializationSchema extends KafkaRecordDeserializationSchema[util.Map[String, AnyRef]] {

  override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]], out: Collector[util.Map[String, AnyRef]]): Unit = {
    val partition = new Integer(record.partition())
    val parsedString = new String(record.value(), StandardCharsets.UTF_8)
    val recordMap = new Gson().fromJson(parsedString, new util.HashMap[String, AnyRef]().getClass).asScala ++ Map("partition" -> partition.asInstanceOf[AnyRef])
    out.collect(recordMap.asJava)
  }

  override def getProducedType: TypeInformation[util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])
}

class MapSerializationSchema(topic: String, key: Option[String] = None) extends KafkaRecordSerializationSchema[util.Map[String, AnyRef]] {

  override def serialize(element: util.Map[String, AnyRef], context: KafkaRecordSerializationSchema.KafkaSinkContext, timestamp: java.lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
    val out = new Gson().toJson(element)
    key.map { kafkaKey =>
      new ProducerRecord[Array[Byte], Array[Byte]](topic, kafkaKey.getBytes(StandardCharsets.UTF_8), out.getBytes(StandardCharsets.UTF_8))
    }.getOrElse(new ProducerRecord[Array[Byte], Array[Byte]](topic, out.getBytes(StandardCharsets.UTF_8)))
  }
}
