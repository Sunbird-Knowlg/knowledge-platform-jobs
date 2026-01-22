package org.sunbird.job.connector

import java.util
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.connector.kafka.sink.KafkaSink
import org.apache.flink.connector.base.DeliveryGuarantee
import org.sunbird.job.BaseJobConfig
import org.sunbird.job.domain.reader.JobRequest
import org.sunbird.job.serde.{JobRequestDeserializationSchema, JobRequestSerializationSchema, MapDeserializationSchema, MapSerializationSchema, StringDeserializationSchema, StringSerializationSchema}

import scala.reflect.ClassTag

class FlinkKafkaConnector(config: BaseJobConfig) extends Serializable {

  def kafkaMapSource(kafkaTopic: String): KafkaSource[util.Map[String, AnyRef]] = {
    val builder = KafkaSource.builder[util.Map[String, AnyRef]]()
      .setBootstrapServers(config.kafkaBrokerServers)
      .setTopics(kafkaTopic)
      .setGroupId(config.groupId)
      .setStartingOffsets(OffsetsInitializer.committedOffsets())
      .setDeserializer(new MapDeserializationSchema())
      .setProperties(config.kafkaConsumerProperties)
    builder.build()
  }

  def kafkaMapSink(kafkaTopic: String): KafkaSink[util.Map[String, AnyRef]] = {
    KafkaSink.builder[util.Map[String, AnyRef]]()
      .setBootstrapServers(config.kafkaBrokerServers)
      .setRecordSerializer(new MapSerializationSchema(kafkaTopic))
      .setKafkaProducerConfig(config.kafkaProducerProperties)
      .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
      .build()
  }

  def kafkaStringSource(kafkaTopic: String): KafkaSource[String] = {
    val builder = KafkaSource.builder[String]()
      .setBootstrapServers(config.kafkaBrokerServers)
      .setTopics(kafkaTopic)
      .setGroupId(config.groupId)
      .setStartingOffsets(OffsetsInitializer.committedOffsets())
      .setDeserializer(new StringDeserializationSchema())
      .setProperties(config.kafkaConsumerProperties)
    builder.build()
  }

  def kafkaStringSink(kafkaTopic: String): KafkaSink[String] = {
    KafkaSink.builder[String]()
      .setBootstrapServers(config.kafkaBrokerServers)
      .setRecordSerializer(new StringSerializationSchema(kafkaTopic))
      .setKafkaProducerConfig(config.kafkaProducerProperties)
      .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
      .build()
  }

  def kafkaJobRequestSource[T <: JobRequest](kafkaTopic: String)(implicit ct: ClassTag[T]): KafkaSource[T] = {
    val builder = KafkaSource.builder[T]()
      .setBootstrapServers(config.kafkaBrokerServers)
      .setTopics(kafkaTopic)
      .setGroupId(config.groupId)
      .setStartingOffsets(OffsetsInitializer.committedOffsets())
      .setDeserializer(new JobRequestDeserializationSchema[T])
      .setProperties(config.kafkaConsumerProperties)
    builder.build()
  }

  def kafkaJobRequestSink[T <: JobRequest](kafkaTopic: String)(implicit m: Manifest[T]): KafkaSink[T] = {
    KafkaSink.builder[T]()
      .setBootstrapServers(config.kafkaBrokerServers)
      .setRecordSerializer(new JobRequestSerializationSchema[T](kafkaTopic))
      .setKafkaProducerConfig(config.kafkaProducerProperties)
      .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
      .build()
  }
}
