package org.sunbird.job.exception

class APIException(message: String, cause: Throwable) extends Exception(message, cause)

class CassandraException(message: String, cause: Throwable) extends Exception(message, cause)

class ElasticSearchException(message: String, cause: Throwable) extends Exception(message, cause)

class KafkaClientException(message: String, cause: Throwable= null) extends Exception(message, cause)

class EventException(message: String, cause: Throwable = null) extends Exception(message, cause)
