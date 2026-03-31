package org.sunbird.job.connector

import java.util
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.connector.kafka.sink.{KafkaSink, KafkaRecordSerializationSchema}
import org.apache.flink.connector.base.DeliveryGuarantee
import org.apache.kafka.clients.consumer.OffsetResetStrategy
import org.sunbird.job.BaseJobConfig
import org.sunbird.job.domain.reader.JobRequest
import org.sunbird.job.serde.{
  JobRequestDeserializationSchema, JobRequestSerializationSchema,
  MapDeserializationSchema, MapSerializationSchema,
  StringDeserializationSchema, StringSerializationSchema
}

class FlinkKafkaConnector(config: BaseJobConfig) extends Serializable {

  def kafkaMapSource(kafkaTopic: String): KafkaSource[util.Map[String, AnyRef]] =
    KafkaSource.builder[util.Map[String, AnyRef]]()
      .setBootstrapServers(config.kafkaBrokerServers)
      .setTopics(kafkaTopic)
      .setGroupId(config.groupId)
      .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
      .setDeserializer(new MapDeserializationSchema())
      .setProperties(config.kafkaConsumerProperties)
      .build()

  def kafkaMapSink(kafkaTopic: String): KafkaSink[util.Map[String, AnyRef]] =
    KafkaSink.builder[util.Map[String, AnyRef]]()
      .setBootstrapServers(config.kafkaBrokerServers)
      .setRecordSerializer(new MapSerializationSchema(kafkaTopic))
      .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
      .setKafkaProducerConfig(config.kafkaProducerProperties)
      .build()

  def kafkaStringSource(kafkaTopic: String): KafkaSource[String] =
    KafkaSource.builder[String]()
      .setBootstrapServers(config.kafkaBrokerServers)
      .setTopics(kafkaTopic)
      .setGroupId(config.groupId)
      .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
      .setDeserializer(new StringDeserializationSchema())
      .setProperties(config.kafkaConsumerProperties)
      .build()

  def kafkaStringSink(kafkaTopic: String): KafkaSink[String] =
    KafkaSink.builder[String]()
      .setBootstrapServers(config.kafkaBrokerServers)
      .setRecordSerializer(new StringSerializationSchema(kafkaTopic))
      .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
      .setKafkaProducerConfig(config.kafkaProducerProperties)
      .build()

  def kafkaJobRequestSource[T <: JobRequest](kafkaTopic: String)(implicit m: Manifest[T]): KafkaSource[T] =
    KafkaSource.builder[T]()
      .setBootstrapServers(config.kafkaBrokerServers)
      .setTopics(kafkaTopic)
      .setGroupId(config.groupId)
      .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
      .setDeserializer(new JobRequestDeserializationSchema[T])
      .setProperties(config.kafkaConsumerProperties)
      .build()

  def kafkaJobRequestSink[T <: JobRequest](kafkaTopic: String)(implicit m: Manifest[T]): KafkaSink[T] =
    KafkaSink.builder[T]()
      .setBootstrapServers(config.kafkaBrokerServers)
      .setRecordSerializer(new JobRequestSerializationSchema[T](kafkaTopic))
      .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
      .setKafkaProducerConfig(config.kafkaProducerProperties)
      .build()
}
