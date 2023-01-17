package org.sunbird.job.cspmigrator.helpers

import com.datastax.driver.core.querybuilder.QueryBuilder
import org.apache.commons.io.{FileUtils, FilenameUtils}
import org.apache.commons.lang3.StringUtils
import org.neo4j.driver.v1.StatementResult
import org.slf4j.LoggerFactory
import org.sunbird.job.Metrics
import org.sunbird.job.cspmigrator.domain.Event
import org.sunbird.job.cspmigrator.task.CSPMigratorConfig
import org.sunbird.job.domain.`object`.DefinitionCache
import org.sunbird.job.exception.{InvalidInputException, ServerException}
import org.sunbird.job.util.CSPMetaUtil.updateAbsolutePath
import org.sunbird.job.util.{CassandraUtil, CloudStorageUtil, HttpUtil, JSONUtil, Neo4JUtil, ScalaJsonUtil, Slug}

import java.io.{File, IOException}
import java.net.URL
import java.util

trait MigrationObjectUpdater extends URLExtractor {

  private[this] val logger = LoggerFactory.getLogger(classOf[MigrationObjectUpdater])

  def updateContentBody(identifier: String, ecmlBody: String, config: CSPMigratorConfig)(implicit cassandraUtil: CassandraUtil): Unit = {
    val updateQuery = QueryBuilder.update(config.contentKeyspaceName, config.contentTableName)
      .where(QueryBuilder.eq("content_id", identifier))
      .`with`(QueryBuilder.set("body", QueryBuilder.fcall("textAsBlob", ecmlBody)))
      logger.info(s"""MigrationObjectUpdater:: updateContentBody:: Updating Content Body in Cassandra For $identifier : ${updateQuery.toString}""")
      val result = cassandraUtil.upsert(updateQuery.toString)
      if (result) logger.info(s"""MigrationObjectUpdater:: updateContentBody:: Content Body Updated Successfully For $identifier""")
      else {
        logger.error(s"""MigrationObjectUpdater:: updateContentBody:: Content Body Update Failed For $identifier""")
        throw new InvalidInputException(s"""Content Body Update Failed For $identifier""")
      }
  }

  def updateAssessmentItemData(identifier: String, updatedData: Map[String, String], config: CSPMigratorConfig)(implicit cassandraUtil: CassandraUtil): Unit = {
    val updateQuery = QueryBuilder.update(config.contentKeyspaceName, config.assessmentTableName)
      .where(QueryBuilder.eq("question_id", identifier))
      .`with`(QueryBuilder.set("body", QueryBuilder.fcall("textAsBlob", updatedData.getOrElse("body", null))))
      .and(QueryBuilder.set("question", QueryBuilder.fcall("textAsBlob", updatedData.getOrElse("question", null))))
      .and(QueryBuilder.set("editorstate", QueryBuilder.fcall("textAsBlob", updatedData.getOrElse("editorstate", null))))
      .and(QueryBuilder.set("solutions", QueryBuilder.fcall("textAsBlob", updatedData.getOrElse("solutions", null))))

    logger.info(s"""MigrationObjectUpdater:: updateAssessmentItemData:: Updating Assessment Body in Cassandra For $identifier : ${updateQuery.toString}""")
    val result = cassandraUtil.upsert(updateQuery.toString)
    if (result) logger.info(s"""MigrationObjectUpdater:: updateAssessmentItemData:: Assessment Body Updated Successfully For $identifier""")
    else {
      logger.error(s"""MigrationObjectUpdater:: updateAssessmentItemData:: Assessment Body Update Failed For $identifier""")
      throw new InvalidInputException(s"""Assessment Body Update Failed For $identifier""")
    }
  }

  def updateCollectionHierarchy(identifier: String, hierarchy: String, config: CSPMigratorConfig)(implicit cassandraUtil: CassandraUtil): Unit = {
    val updateQuery = QueryBuilder.update(config.hierarchyKeyspaceName, config.hierarchyTableName)
      .where(QueryBuilder.eq("identifier", identifier))
      .`with`(QueryBuilder.set("hierarchy", hierarchy))
    logger.info(s"""MigrationObjectUpdater:: updateCollectionHierarchy:: Updating Hierarchy in Cassandra For $identifier : ${updateQuery.toString}""")
    val result = cassandraUtil.upsert(updateQuery.toString)
    if (result) logger.info(s"""MigrationObjectUpdater:: updateCollectionHierarchy:: Hierarchy Updated Successfully For $identifier""")
    else {
      logger.error(s"""MigrationObjectUpdater:: updateCollectionHierarchy:: Hierarchy Update Failed For $identifier""")
      throw new InvalidInputException(s"""Hierarchy Update Failed For $identifier""")
    }
  }

  def updateQuestionSetHierarchy(identifier: String, hierarchy: String, config: CSPMigratorConfig)(implicit cassandraUtil: CassandraUtil): Unit = {
    val updateQuery = QueryBuilder.update(config.qsHierarchyKeyspaceName, config.qsHierarchyTableName)
      .where(QueryBuilder.eq("identifier", identifier))
      .`with`(QueryBuilder.set("hierarchy", hierarchy))
    logger.info(s"""MigrationObjectUpdater:: updateQuestionSetHierarchy:: Updating Hierarchy in Cassandra For $identifier : ${updateQuery.toString}""")
    val result = cassandraUtil.upsert(updateQuery.toString)
    if (result) logger.info(s"""MigrationObjectUpdater:: updateQuestionSetHierarchy:: Hierarchy Updated Successfully For $identifier""")
    else {
      logger.error(s"""MigrationObjectUpdater:: updateQuestionSetHierarchy:: Hierarchy Update Failed For $identifier""")
      throw new InvalidInputException(s"""Hierarchy Update Failed For $identifier""")
    }
  }

  def updateNeo4j(updatedMetadata: Map[String, AnyRef], event: Event)(definitionCache: DefinitionCache, neo4JUtil: Neo4JUtil, config: CSPMigratorConfig): Unit = {
    logger.info(s"""MigrationObjectUpdater:: process:: ${event.identifier} - ${event.objectType} updated fields data:: $updatedMetadata""")
    val metadataUpdateQuery = metaDataQuery(event.objectType, updatedMetadata)(definitionCache, config)
    val query = s"""MATCH (n:domain{IL_UNIQUE_ID:"${event.identifier}"}) SET $metadataUpdateQuery return n;"""
    logger.info(s"""MigrationObjectUpdater:: process:: ${event.identifier} - ${event.objectType} updated fields :: Query: """ + query)
    val sResult: StatementResult = neo4JUtil.executeQuery(query)
    if(sResult !=null) logger.info("MigrationObjectUpdater:: process:: sResult:: " + sResult)
    logger.info("MigrationObjectUpdater:: process:: static fields migration completed for " + event.identifier)
  }


  def extractAndValidateUrls(identifier: String, contentString: String, config: CSPMigratorConfig, httpUtil: HttpUtil, cloudStorageUtil: CloudStorageUtil): String = {
    val extractedUrls: List[String] = extarctUrls(contentString)
    logger.info("MigrationObjectUpdater::extractAndValidateUrls:: extractedUrls : " + extractedUrls)
    if(extractedUrls.nonEmpty) {
      var tempContentString: String = contentString

      val migratedString = if(config.copyMissingFiles) {
        extractedUrls.toSet[String].foreach(urlString => {
          // TODO: call a method to validate the url, upload to cloud set the url to migrated value
          val tempUrlString = handleGoogleDriveMetadata(urlString, identifier, config, httpUtil, cloudStorageUtil)

          config.keyValueMigrateStrings.keySet().toArray().map(migrateDomain => {
            if (StringUtils.isNotBlank(tempUrlString) && tempUrlString.contains(migrateDomain.asInstanceOf[String])) {
              tempContentString = StringUtils.replace(tempContentString, urlString, tempUrlString)

              val migrateValue: String = StringUtils.replaceEach(tempUrlString, config.keyValueMigrateStrings.keySet().toArray().map(_.asInstanceOf[String]), config.keyValueMigrateStrings.values().toArray().map(_.asInstanceOf[String]))
              verifyFile(identifier, tempUrlString, migrateValue, migrateDomain.asInstanceOf[String], config)(httpUtil, cloudStorageUtil)
            }
          })
        })
        tempContentString
      }else{
        extractedUrls.toSet[String].foreach(urlString => {
          logger.info("MigrationObjectUpdater::extractAndValidateUrls:: urlString : " + urlString)
          val tempUrlString = handleGoogleDriveMetadata(urlString, identifier, config, httpUtil, cloudStorageUtil)
          logger.info("MigrationObjectUpdater::extractAndValidateUrls:: tempUrlString : " + tempUrlString)
          if(StringUtils.isNotBlank(tempUrlString))
            tempContentString = StringUtils.replace(tempContentString, urlString, tempUrlString)
        })
        tempContentString
      }

      StringUtils.replaceEach(migratedString, config.keyValueMigrateStrings.keySet().toArray().map(_.asInstanceOf[String]), config.keyValueMigrateStrings.values().toArray().map(_.asInstanceOf[String]))
    } else contentString
  }


  def downloadFile(downloadPath: String, fileUrl: String): File = try {
    createDirectory(downloadPath)
    val file = new File(downloadPath + File.separator + FilenameUtils.getName(fileUrl))
    FileUtils.copyURLToFile(new URL(fileUrl), file)
    file
  } catch {
    case e: IOException =>
      e.printStackTrace()
      throw new ServerException("ERR_INVALID_FILE_URL", "File not found in the old path to migrate: " + fileUrl)
  }

  private def createDirectory(directoryName: String): Unit = {
    val theDir = new File(directoryName)
    if (!theDir.exists) theDir.mkdirs
  }

  def finalizeMigration(migratedMap: Map[String, AnyRef], event: Event, metrics: Metrics, config: CSPMigratorConfig)(defCache: DefinitionCache, neo4JUtil: Neo4JUtil): Unit = {
    updateNeo4j(migratedMap + ("migrationVersion" -> config.migrationVersion.asInstanceOf[AnyRef]), event)(defCache, neo4JUtil, config)
    logger.info("MigrationObjectUpdater::finalizeMigration:: CSP migration operation completed for : " + event.identifier)
    metrics.incCounter(config.successEventCount)
  }

  def verifyFile(identifier: String, originalUrl: String, migrateUrl: String, migrateDomain: String, config: CSPMigratorConfig)(implicit httpUtil: HttpUtil, cloudStorageUtil: CloudStorageUtil): Unit = {
    val updateMigrateUrl = updateAbsolutePath(migrateUrl)(config)
    logger.info("MigrationObjectUpdater::verifyFile:: originalUrl :: " + originalUrl + " || updateMigrateUrl:: " + updateMigrateUrl)
    if(httpUtil.getSize(updateMigrateUrl) <= 0) {
      if (config.copyMissingFiles) {
        if(FilenameUtils.getExtension(originalUrl) != null && !FilenameUtils.getExtension(originalUrl).isBlank && FilenameUtils.getExtension(originalUrl).nonEmpty) {
          // code to download file from old cloud path and upload to new cloud path
          val downloadedFile: File = downloadFile(s"/tmp/$identifier", originalUrl)
          val exDomain: String = originalUrl.replace(migrateDomain, "")
          val folderName: String = exDomain.substring(1, exDomain.indexOf(FilenameUtils.getName(originalUrl)) - 1)
          cloudStorageUtil.uploadFile(folderName, downloadedFile)
        }
      } else throw new ServerException("ERR_NEW_PATH_NOT_FOUND", "File not found in the new path to migrate: " + updateMigrateUrl)
    }
  }


  def metaDataQuery(objectType: String, objMetadata: Map[String, AnyRef])(definitionCache: DefinitionCache, config: CSPMigratorConfig): String = {
    val version = if(objectType.equalsIgnoreCase("itemset")) "2.0" else "1.0"
    val definition = definitionCache.getDefinition(objectType, version, config.definitionBasePath)
    val metadata = objMetadata - ("IL_UNIQUE_ID", "identifier", "IL_FUNC_OBJECT_TYPE", "IL_SYS_NODE_TYPE", "pkgVersion", "lastStatusChangedOn", "lastUpdatedOn", "status", "objectType", "publish_type")
    metadata.map(prop => {
      if (null == prop._2) s"n.${prop._1}=${prop._2}"
      else if (definition.objectTypeProperties.contains(prop._1)) {
        prop._2 match {
          case _: Map[String, AnyRef] =>
            val strValue = JSONUtil.serialize(ScalaJsonUtil.serialize(prop._2))
            s"""n.${prop._1}=$strValue"""
          case _: util.Map[String, AnyRef] =>
            val strValue = JSONUtil.serialize(JSONUtil.serialize(prop._2))
            s"""n.${prop._1}=$strValue"""
          case _ =>
            val strValue = JSONUtil.serialize(prop._2)
            s"""n.${prop._1}=$strValue"""
        }
      } else {
        prop._2 match {
          case _: Map[String, AnyRef] =>
            val strValue = JSONUtil.serialize(ScalaJsonUtil.serialize(prop._2))
            s"""n.${prop._1}=$strValue"""
          case _: util.Map[String, AnyRef] =>
            val strValue = JSONUtil.serialize(JSONUtil.serialize(prop._2))
            s"""n.${prop._1}=$strValue"""
          case _: List[String] =>
            val strValue = ScalaJsonUtil.serialize(prop._2)
            s"""n.${prop._1}=$strValue"""
          case _: util.List[String] =>
            val strValue = JSONUtil.serialize(prop._2)
            s"""n.${prop._1}=$strValue"""
          case _ =>
            val strValue = JSONUtil.serialize(prop._2)
            s"""n.${prop._1}=$strValue"""
        }
      }
    }).mkString(",")
  }

  @throws[Exception]
  private def getFile(identifier: String, fileUrl: String, config: CSPMigratorConfig, httpUtil: HttpUtil): File = {
    try {
      val fileId = fileUrl.split("download&id=")(1)
      if (StringUtils.isBlank(fileId)) {
        logger.info("Invalid fileUrl received for : " + identifier + " | fileUrl : " + fileUrl)
        null
      } else {
        GoogleDriveUtil.downloadFile(fileId, getBasePath(identifier, config))(config)
      }
    } catch {
      case e: ServerException =>{
        logger.info("Invalid fileUrl received for : " + identifier + " | fileUrl : " + fileUrl + "Exception is : " + e.getMessage)
        null
      }
      case ex: Exception => {
        logger.info("Invalid fileUrl received for : " + identifier + " | fileUrl : " + fileUrl + "Exception is : " + ex.getMessage)
        null
      }
    }
  }

  private def getBasePath(objectId: String, config: CSPMigratorConfig): String = {
    if (StringUtils.isNotBlank(objectId)) config.temp_file_location + File.separator + objectId + File.separator + "_temp_" + System.currentTimeMillis
    else config.temp_file_location + File.separator + "_temp_" + System.currentTimeMillis
  }

  def handleGoogleDriveMetadata(fileUrl: String, contentId: String, config: CSPMigratorConfig, httpUtil: HttpUtil, cloudStorageUtil: CloudStorageUtil): String = {
    if (StringUtils.isNotBlank(fileUrl) && fileUrl.contains("drive.google.com")) {
      val file = getFile(contentId, fileUrl, config, httpUtil)
      logger.info("ContentAutoCreator :: update :: Icon downloaded for : " + contentId + " | appIconUrl : " + fileUrl)

      if (null == file || !file.exists) {
        logger.info("Error Occurred while downloading appIcon file for " + contentId + " | File Url : " + fileUrl)
        null
      } else {
        val urls = uploadArtifact(file, contentId, config, cloudStorageUtil)
        val url = if (null != urls && StringUtils.isNotBlank(urls(1))) {
          val blobUrl = urls(1)
          logger.info("CSPNeo4jMigrator :: handleGoogleDriveMetadata :: Icon Uploaded Successfully to cloud for : " + contentId + " | appIconUrl : " + fileUrl + " | appIconBlobUrl : " + blobUrl)
          FileUtils.deleteQuietly(file)
          blobUrl
        } else null
        url
      }
    } else fileUrl
  }

  private def uploadArtifact(uploadedFile: File, identifier: String, config: CSPMigratorConfig, cloudStorageUtil: CloudStorageUtil) = {
    try {
      var folder = config.contentFolder
      folder = folder + "/" + Slug.makeSlug(identifier, isTransliterate = true) + "/" + config.artifactFolder
      cloudStorageUtil.uploadFile(folder, uploadedFile, Option(true))
    } catch {
      case e: Exception => e.printStackTrace()
        logger.info("ContentAutoCreator :: uploadArtifact ::  Exception occurred while uploading artifact for : " + identifier + "Exception is : " + e.getMessage)
        throw new ServerException("ERR_CONTENT_UPLOAD_FILE", "Error while uploading the File.", e)
    }
  }

}
