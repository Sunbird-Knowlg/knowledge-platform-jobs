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

object FlinkKafkaConnector {
  /**
   * Derive the no-committed-offset fallback strategy from `kafka.auto.offset.reset` config.
   * The old FlinkKafkaConsumer (Flink 1.13) passed the Properties object directly to the Kafka
   * client, so the broker-side `auto.offset.reset` property controlled the fallback.  When the
   * property was absent the Kafka default of LATEST applied.  The new KafkaSource API requires
   * an explicit OffsetsInitializer; we reproduce the original behaviour here.
   */
  def resolveOffsetResetStrategy(autoOffsetReset: Option[String]): OffsetResetStrategy =
    autoOffsetReset
      .map(_.toUpperCase)
      .flatMap(s => scala.util.Try(OffsetResetStrategy.valueOf(s)).toOption)
      .getOrElse(OffsetResetStrategy.LATEST)
}

class FlinkKafkaConnector(config: BaseJobConfig) extends Serializable {

  private val noCommittedOffsetStrategy: OffsetResetStrategy =
    FlinkKafkaConnector.resolveOffsetResetStrategy(config.kafkaAutoOffsetReset)

  def kafkaMapSource(kafkaTopic: String): KafkaSource[util.Map[String, AnyRef]] =
    KafkaSource.builder[util.Map[String, AnyRef]]()
      .setBootstrapServers(config.kafkaBrokerServers)
      .setTopics(kafkaTopic)
      .setGroupId(config.groupId)
      .setStartingOffsets(OffsetsInitializer.committedOffsets(noCommittedOffsetStrategy))
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
      .setStartingOffsets(OffsetsInitializer.committedOffsets(noCommittedOffsetStrategy))
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
      .setStartingOffsets(OffsetsInitializer.committedOffsets(noCommittedOffsetStrategy))
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
