package org.sunbird.job.util

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.kafka.clients.consumer.{Consumer, ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{LongDeserializer, LongSerializer, StringDeserializer, StringSerializer}
import org.neo4j.driver.v1.exceptions.ClientException

import java.util.Properties


class KafkaClientUtil {
	val config: Config = ConfigFactory.load("base-config.conf").withFallback(ConfigFactory.systemEnvironment())
	private val BOOTSTRAP_SERVERS = config.getString("kafka.urls")
	private val producer = createProducer()
	private val consumer = createConsumer()

	protected def getProducer: Producer[Long, String] = producer
	protected def getConsumer: Consumer[Long, String] = consumer

	@throws[Exception]
	def send(event: String, topic: String): Unit = {
//		if (!config.getBoolean("kafka.topic.send.enable")) return
		if (validate(topic))
			getProducer.send(new ProducerRecord[Long, String](topic, event))
		else {
			println("Topic with name: " + topic + ", does not exists.")
			throw new ClientException("TOPIC_NOT_FOUND_EXCEPTION", "Topic with name: " + topic + ", does not exists.")

		}
	}

	@throws[Exception]
	def validate(topic: String): Boolean = {
		val topics = getConsumer.listTopics
		topics.keySet.contains(topic)
	}

	private def createProducer(): KafkaProducer[Long, String] = {
		new KafkaProducer[Long, String](new Properties() {
			{
				put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS)
				put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaClientProducer")
				put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[LongSerializer].getName)
				put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
			}
		})
	}

	private def createConsumer(): KafkaConsumer[Long, String] = {
		new KafkaConsumer[Long, String](new Properties() {
			{
				put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS)
				put(ConsumerConfig.CLIENT_ID_CONFIG, "KafkaClientConsumer")
				put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[LongDeserializer].getName)
				put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
			}
		})
	}
}
