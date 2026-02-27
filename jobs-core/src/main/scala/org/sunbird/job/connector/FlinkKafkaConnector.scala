package org.sunbird.job.connector

import java.util
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.connector.kafka.sink.{KafkaRecordSerializationSchema, KafkaSink}
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer.Semantic
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.sunbird.job.BaseJobConfig
import org.sunbird.job.domain.reader.JobRequest
import org.sunbird.job.serde.{JobRequestDeserializationSchema, JobRequestSerializationSchema, MapDeserializationSchema, MapSerializationSchema, StringDeserializationSchema, StringSerializationSchema}

import scala.reflect.Manifest

class FlinkKafkaConnector(config: BaseJobConfig) extends Serializable {

  def kafkaMapSource(kafkaTopic: String): SourceFunction[util.Map[String, AnyRef]] = {
    new FlinkKafkaConsumer[util.Map[String, AnyRef]](kafkaTopic, new MapDeserializationSchema, config.kafkaConsumerProperties)
  }

  def kafkaMapSink(kafkaTopic: String): SinkFunction[util.Map[String, AnyRef]] = {
    new FlinkKafkaProducer[util.Map[String, AnyRef]](kafkaTopic, new MapSerializationSchema(kafkaTopic), config.kafkaProducerProperties, Semantic.AT_LEAST_ONCE)
  }

  def kafkaStringSource(kafkaTopic: String): SourceFunction[String] = {
    new FlinkKafkaConsumer[String](kafkaTopic, new StringDeserializationSchema, config.kafkaConsumerProperties)
  }

  def kafkaStringSink(kafkaTopic: String): SinkFunction[String] = {
    new FlinkKafkaProducer[String](kafkaTopic, new StringSerializationSchema(kafkaTopic), config.kafkaProducerProperties, Semantic.AT_LEAST_ONCE)
  }

  def kafkaJobRequestSource[T <: JobRequest](kafkaTopic: String)(implicit m: Manifest[T]): SourceFunction[T] = {
    new FlinkKafkaConsumer[T](kafkaTopic, new JobRequestDeserializationSchema[T], config.kafkaConsumerProperties)
  }

  def kafkaJobRequestSink[T <: JobRequest](kafkaTopic: String)(implicit m: Manifest[T]): SinkFunction[T] = {
    new FlinkKafkaProducer[T](kafkaTopic,
      new JobRequestSerializationSchema[T](kafkaTopic), config.kafkaProducerProperties, Semantic.AT_LEAST_ONCE)
  }

  // New KafkaSource and KafkaSink methods for Flink 1.15+ (Production use)
  def kafkaMapSourceV2(kafkaTopic: String): KafkaSource[util.Map[String, AnyRef]] = {
    KafkaSource.builder[util.Map[String, AnyRef]]()
      .setBootstrapServers(config.kafkaBrokerServers)
      .setTopics(kafkaTopic)
      .setGroupId(config.groupId)
      .setStartingOffsets(OffsetsInitializer.committedOffsets())
      .setDeserializer(new MapDeserializationSchema)
      .setProperties(config.kafkaConsumerProperties)
      .build()
  }

  def kafkaStringSourceV2(kafkaTopic: String): KafkaSource[String] = {
    KafkaSource.builder[String]()
      .setBootstrapServers(config.kafkaBrokerServers)
      .setTopics(kafkaTopic)
      .setGroupId(config.groupId)
      .setStartingOffsets(OffsetsInitializer.committedOffsets())
      .setDeserializer(new StringDeserializationSchema)
      .setProperties(config.kafkaConsumerProperties)
      .build()
  }

  def kafkaJobRequestSourceV2[T <: JobRequest](kafkaTopic: String)(implicit m: Manifest[T]): KafkaSource[T] = {
    KafkaSource.builder[T]()
      .setBootstrapServers(config.kafkaBrokerServers)
      .setTopics(kafkaTopic)
      .setGroupId(config.groupId)
      .setStartingOffsets(OffsetsInitializer.committedOffsets())
      .setDeserializer(new JobRequestDeserializationSchema[T])
      .setProperties(config.kafkaConsumerProperties)
      .build()
  }

  def kafkaMapSinkV2(kafkaTopic: String): KafkaSink[util.Map[String, AnyRef]] = {
    KafkaSink.builder[util.Map[String, AnyRef]]()
      .setBootstrapServers(config.kafkaBrokerServers)
      .setRecordSerializer(KafkaRecordSerializationSchema.builder[util.Map[String, AnyRef]]()
        .setTopic(kafkaTopic)
        .setValueSerializationSchema(new MapSerializationSchema(kafkaTopic))
        .build())
      .setKafkaProducerConfig(config.kafkaProducerProperties)
      .build()
  }

  def kafkaStringSinkV2(kafkaTopic: String): KafkaSink[String] = {
    KafkaSink.builder[String]()
      .setBootstrapServers(config.kafkaBrokerServers)
      .setRecordSerializer(KafkaRecordSerializationSchema.builder[String]()
        .setTopic(kafkaTopic)
        .setValueSerializationSchema(new StringSerializationSchema(kafkaTopic))
        .build())
      .setKafkaProducerConfig(config.kafkaProducerProperties)
      .build()
  }

  def kafkaJobRequestSinkV2[T <: JobRequest](kafkaTopic: String)(implicit m: Manifest[T]): KafkaSink[T] = {
    KafkaSink.builder[T]()
      .setBootstrapServers(config.kafkaBrokerServers)
      .setRecordSerializer(KafkaRecordSerializationSchema.builder[T]()
        .setTopic(kafkaTopic)
        .setValueSerializationSchema(new JobRequestSerializationSchema[T](kafkaTopic))
        .build())
      .setKafkaProducerConfig(config.kafkaProducerProperties)
      .build()
  }
}
