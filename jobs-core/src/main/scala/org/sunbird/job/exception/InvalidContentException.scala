package org.sunbird.job.exception

import org.slf4j.LoggerFactory

class InvalidContentException(message: String, cause: Throwable = null) extends Exception(message, cause) {

  private[this] val logger = LoggerFactory.getLogger(classOf[InvalidContentException])

  def this(message: String, event: Map[String, Any], cause: Throwable) = {
    this(message, cause)
    val partitionNum = event.getOrElse("partition", null)
    val offset = event.getOrElse("offset", null)
    logger.error(s"Error while processing event for Partition: $partitionNum and Offset: $offset. Error : $message", cause)
  }

}
