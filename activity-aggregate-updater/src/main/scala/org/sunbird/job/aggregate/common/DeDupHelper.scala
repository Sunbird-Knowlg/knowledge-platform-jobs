package org.sunbird.job.aggregate.common

import java.security.MessageDigest

object DeDupHelper {

  def getMessageId(collectionId: String, batchId: String, userId: String, contentId: String, status: Int): String = {
    val key = Array(collectionId, batchId, userId, contentId, status).mkString("|")
    MessageDigest.getInstance("MD5").digest(key.getBytes).map("%02X".format(_)).mkString;
  }

}
