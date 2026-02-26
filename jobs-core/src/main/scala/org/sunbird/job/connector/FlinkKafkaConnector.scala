package org.sunbird.job.connector

import java.util
import org.apache.flink.connector.base.DeliveryGuarantee
import org.apache.flink.connector.kafka.sink.KafkaSink
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.kafka.clients.consumer.OffsetResetStrategy
import org.sunbird.job.BaseJobConfig
import org.sunbird.job.domain.reader.JobRequest
import org.sunbird.job.serde.{JobRequestDeserializationSchema, JobRequestSerializationSchema, MapDeserializationSchema, MapSerializationSchema, StringDeserializationSchema, StringSerializationSchema}

class FlinkKafkaConnector(config: BaseJobConfig) extends Serializable {

  def kafkaMapSource(kafkaTopic: String): KafkaSource[util.Map[String, AnyRef]] =
    KafkaSource.builder[util.Map[String, AnyRef]]()
      .setProperties(config.kafkaConsumerProperties)
      .setTopics(kafkaTopic)
      .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
      .setDeserializer(new MapDeserializationSchema)
      .build()

  def kafkaMapSink(kafkaTopic: String): KafkaSink[util.Map[String, AnyRef]] =
    KafkaSink.builder[util.Map[String, AnyRef]]()
      .setKafkaProducerConfig(config.kafkaProducerProperties)
      .setRecordSerializer(new MapSerializationSchema(kafkaTopic))
      .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
      .build()

  def kafkaStringSource(kafkaTopic: String): KafkaSource[String] =
    KafkaSource.builder[String]()
      .setProperties(config.kafkaConsumerProperties)
      .setTopics(kafkaTopic)
      .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
      .setDeserializer(new StringDeserializationSchema)
      .build()

  def kafkaStringSink(kafkaTopic: String): KafkaSink[String] =
    KafkaSink.builder[String]()
      .setKafkaProducerConfig(config.kafkaProducerProperties)
      .setRecordSerializer(new StringSerializationSchema(kafkaTopic))
      .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
      .build()

  def kafkaJobRequestSource[T <: JobRequest](kafkaTopic: String)(implicit m: Manifest[T]): KafkaSource[T] =
    KafkaSource.builder[T]()
      .setProperties(config.kafkaConsumerProperties)
      .setTopics(kafkaTopic)
      .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
      .setDeserializer(new JobRequestDeserializationSchema[T])
      .build()

  def kafkaJobRequestSink[T <: JobRequest](kafkaTopic: String)(implicit m: Manifest[T]): KafkaSink[T] =
    KafkaSink.builder[T]()
      .setKafkaProducerConfig(config.kafkaProducerProperties)
      .setRecordSerializer(new JobRequestSerializationSchema[T](kafkaTopic))
      .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
      .build()
}
