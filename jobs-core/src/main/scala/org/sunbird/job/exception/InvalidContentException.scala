package org.sunbird.job.exception

import org.slf4j.LoggerFactory

class InvalidContentException(message: String) extends Exception(message) {

  private[this] val logger = LoggerFactory.getLogger(classOf[InvalidContentException])

  def this(message: String, event: Map[String, Any], cause: Throwable) = {
    this(message)
    val partitionNum = event.getOrElse("partition", null)
    val offset = event.getOrElse("offset", null)
    logger.error(s"Error while processing message for Partition: $partitionNum and Offset: $offset. Error : $message", cause)
  }
}
