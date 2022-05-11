package org.sunbird.job.dialcodecontextupdater.helpers

import org.apache.commons.io.FileUtils
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.sunbird.job.dialcodecontextupdater.domain.Event
import org.sunbird.job.dialcodecontextupdater.task.DialcodeContextUpdaterConfig
import org.sunbird.job.dialcodecontextupdater.util.DialcodeContextUpdaterConstants
import org.sunbird.job.dialcodecontextupdater.domain.Event
import org.sunbird.job.dialcodecontextupdater.task.DialcodeContextUpdaterConfig
import org.sunbird.job.exception.ServerException
import org.sunbird.job.util._

import java.io.File
import java.util
import scala.collection.convert.ImplicitConversions.`map AsJavaMap`

trait DialcodeContextUpdater {

	private[this] val logger = LoggerFactory.getLogger(classOf[DialcodeContextUpdater])

	def process(config: DialcodeContextUpdaterConfig, event: Event, httpUtil: HttpUtil, neo4JUtil: Neo4JUtil, cloudStorageUtil: CloudStorageUtil): Boolean = {
		val stage = event.eData.getOrDefault("stage", "").asInstanceOf[String].trim
		true
	}
}
