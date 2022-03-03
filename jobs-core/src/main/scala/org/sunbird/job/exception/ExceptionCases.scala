package org.sunbird.job.exception

class APIException(message: String, cause: Throwable) extends Exception(message, cause)

class CassandraException(message: String, cause: Throwable) extends Exception(message, cause)

class ElasticSearchException(message: String, cause: Throwable) extends Exception(message, cause)

class ServerException(code: String, msg: String, cause: Throwable = null) extends Exception(msg, cause)