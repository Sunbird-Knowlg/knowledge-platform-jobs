package org.sunbird.publish.util

import java.io.{File, IOException}
import java.net.URL

import org.apache.commons.io.FileUtils
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory

object FileUtil {

	private[this] val logger = LoggerFactory.getLogger(classOf[FileUtil])

	def copyURLToFile(objectId: String, fileUrl: String, suffix: String): Option[File] = try {
		val fileName = getBasePath(objectId) + "/" + suffix
		val file = new File(fileName)
		FileUtils.copyURLToFile(new URL(fileUrl), file)
		Some(file)
	} catch {
		case e: IOException => logger.error("ERR_INVALID_FILE_URL", "Please Provide Valid File Url!")
			None
	}

	def getBasePath(objectId: String): String = {
		if (!StringUtils.isBlank(objectId))
			s"/tmp/$objectId/${System.currentTimeMillis}_temp"
		else ""
	}

}

class FileUtil {}
