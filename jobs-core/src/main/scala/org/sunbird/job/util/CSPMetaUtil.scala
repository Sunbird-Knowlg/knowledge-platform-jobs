package org.sunbird.job.util

import java.util

import org.apache.commons.collections.MapUtils
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.sunbird.job.BaseJobConfig

import scala.collection.JavaConverters._

object CSPMetaUtil {

  private[this] val logger = LoggerFactory.getLogger(classOf[CSPMetaUtil])

  def updateAbsolutePath(data: util.Map[String, AnyRef])(implicit config: BaseJobConfig): util.Map[String, AnyRef] = {
    logger.info("CSPMetaUtil ::: updateAbsolutePath ::: data before url replace :: " + data)
    val relativePathPrefix: String = config.getString("cloudstorage.relative_path_prefix", "")
    val cspMeta: util.List[String] = config.config.getStringList("cloudstorage.metadata.list")
    val absolutePath = config.getString("cloudstorage.read_base_path", "") + java.io.File.separator + config.getString("cloud_storage_container", "")
    val result = if (MapUtils.isNotEmpty(data)) {
      val updatedMeta: util.Map[String, AnyRef] = data.asScala.map(x => if (cspMeta.contains(x._1)) (x._1, x._2.asInstanceOf[String].replace(relativePathPrefix, absolutePath)) else (x._1, x._2)).toMap.asJava
      updatedMeta
    } else data
    logger.info("CSPMetaUtil ::: updateAbsolutePath ::: data after url replace :: " + result)
    result
  }

  def updateAbsolutePath(data: util.List[util.Map[String, AnyRef]])(implicit config: BaseJobConfig): util.List[util.Map[String, AnyRef]] = {
    logger.info("CSPMetaUtil ::: updateAbsolutePath util.List[util.Map[String, AnyRef]] ::: data before url replace :: " + data)
    val relativePathPrefix: String = config.getString("cloudstorage.relative_path_prefix", "")
    val cspMeta: util.List[String] = config.config.getStringList("cloudstorage.metadata.list")
    val absolutePath: String = config.getString("cloudstorage.read_base_path", "") + java.io.File.separator + config.getString("cloud_storage_container", "")
    val result = data.asScala.toList.map(meta => {
      if (MapUtils.isNotEmpty(meta)) {
        val updatedMeta: util.Map[String, AnyRef] = meta.asScala.map(x => if (cspMeta.contains(x._1)) (x._1, getBasePath(x._1, x._2, Array(relativePathPrefix), Array(absolutePath))) else (x._1, x._2)).toMap.asJava
        updatedMeta
      } else meta
    }).asJava
    logger.info("CSPMetaUtil ::: updateAbsolutePath util.List[util.Map[String, AnyRef]] ::: data after url replace :: " + result)
    result
  }

  def updateAbsolutePath(data: String)(implicit config: BaseJobConfig): String = {
    logger.info("CSPMetaUtil ::: updateAbsolutePath String ::: data before url replace :: " + data)
    val relativePathPrefix: String = config.getString("cloudstorage.relative_path_prefix", "")
    val absolutePath = config.getString("cloudstorage.read_base_path", "") + java.io.File.separator + config.getString("cloud_storage_container", "")
    val result = if (StringUtils.isNotEmpty(data)) {
      val updatedData: String = data.replaceAll(relativePathPrefix, absolutePath)
      updatedData
    } else data
    logger.info("CSPMetaUtil ::: updateAbsolutePath String ::: data after url replace :: " + result)
    result
  }

  def updateRelativePath(data: util.Map[String, AnyRef])(implicit config: BaseJobConfig): util.Map[String, AnyRef] = {
    logger.info("CSPMetaUtil ::: updateRelativePath util.Map[String, AnyRef] ::: data before url replace :: " + data)
    val relativePathPrefix: String = config.getString("cloudstorage.relative_path_prefix", "")
    val cspMeta: util.List[String] = config.config.getStringList("cloudstorage.metadata.list")
    val validCSPSource: List[String] = config.config.getStringList("cloudstorage.write_base_path").asScala.toList
    val basePaths: Array[String] = validCSPSource.map(source => source + java.io.File.separator + config.getString("cloud_storage_container", "")).toArray
    val repArray = getReplacementData(basePaths, relativePathPrefix)
    val result = if (MapUtils.isNotEmpty(data)) {
      val updatedMeta: util.Map[String, AnyRef] = data.asScala.map(x => if (cspMeta.contains(x._1)) (x._1, getBasePath(x._1, x._2, basePaths, repArray)) else (x._1, x._2)).toMap.asJava
      updatedMeta
    } else data
    logger.info("CSPMetaUtil ::: updateRelativePath util.Map[String, AnyRef] ::: data after url replace :: " + result)
    result
  }

  def updateRelativePath(query: String)(implicit config: BaseJobConfig): String = {
    logger.info("CSPMetaUtil ::: updateRelativePath ::: query before url replace :: " + query)
    val relativePathPrefix: String = config.getString("cloudstorage.relative_path_prefix", "")
    val validCSPSource: List[String] = config.config.getStringList("cloudstorage.write_base_path").asScala.toList
    val paths: Array[String] = validCSPSource.map(s => s + java.io.File.separator + config.getString("cloud_storage_container", "")).toArray
    val repArray = getReplacementData(paths, relativePathPrefix)
    val result = StringUtils.replaceEach(query, paths, repArray)
    logger.info("CSPMetaUtil ::: updateRelativePath ::: query after url replace :: " + result)
    result
  }

  def updateCloudPath(objList: List[Map[String, AnyRef]])(implicit config: BaseJobConfig): List[Map[String, AnyRef]] = {
    logger.info("CSPMetaUtil ::: updateCloudPath List[Map[String, AnyRef]] ::: data before url replace :: " + objList)
    val cspMeta: util.List[String] = config.config.getStringList("cloudstorage.metadata.list")
    val validCSPSource: List[String] = config.config.getStringList("cloudstorage.write_base_path").asScala.toList
    val paths: Array[String] = validCSPSource.map(s => s + java.io.File.separator + config.getString("cloud_storage_container", "")).toArray
    val newCloudPath: String = config.getString("cloudstorage.read_base_path", "") + java.io.File.separator + config.getString("cloud_storage_container", "")
    val repArray = getReplacementData(paths, newCloudPath)
    val result = objList.map(data => {
      if (null != data && data.nonEmpty) {
        data.map(x => if (cspMeta.contains(x._1)) (x._1, getBasePath(x._1, x._2, paths, repArray)) else (x._1, x._2)).toMap
      } else data
    })
    logger.info("CSPMetaUtil ::: updateCloudPath List[Map[String, AnyRef]] ::: data after url replace :: " + result)
    result
  }

  private def getBasePath(key: String, value: AnyRef, oldPath: Array[String], newPath: Array[String])(implicit config: BaseJobConfig): AnyRef = {
    logger.info(s"CSPMetaUtil ::: getBasePath ::: Updating Path for Key : ${key} & Value : ${value}")
    val res = if (null != value) {
      value match {
        case x: String => if (StringUtils.isNotBlank(x)) StringUtils.replaceEach(x, oldPath, newPath) else x
        case y: Map[String, AnyRef] => {
          val dStr = ScalaJsonUtil.serialize(y)
          val result = StringUtils.replaceEach(dStr, oldPath, newPath)
          val output: Map[String, AnyRef] = ScalaJsonUtil.deserialize[Map[String, AnyRef]](result)
          output
        }
        case z: util.Map[String, AnyRef] => {
          val dStr = ScalaJsonUtil.serialize(z)
          val result = StringUtils.replaceEach(dStr, oldPath, newPath)
          val output: util.Map[String, AnyRef] = ScalaJsonUtil.deserialize[util.Map[String, AnyRef]](result)
          output
        }
      }
    } else value
    logger.info(s"CSPMetaUtil ::: getBasePath ::: Updated Path for Key : ${key} & Updated Value is : ${res}")
    res
  }

  private def getReplacementData(oldPath: Array[String], repStr: String): Array[String] = {
    val repArray = new Array[String](oldPath.length)
    for (i <- oldPath.indices) {
      repArray(i) = repStr
    }
    repArray
  }

}

class CSPMetaUtil {}