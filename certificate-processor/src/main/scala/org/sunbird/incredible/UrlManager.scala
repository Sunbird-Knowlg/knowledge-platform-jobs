package org.sunbird.incredible

import java.net.{MalformedURLException, URL}

import org.apache.commons.lang3.StringUtils
import org.slf4j.{Logger, LoggerFactory}

object UrlManager {
  private val logger = LoggerFactory.getLogger(classOf[UrlManager.type])

  def getSharableUrl(url: String, containerName: String): String = {
    var uri: String = null
    uri = removeQueryParams(url)
    uri = fetchFileFromUrl(uri)
    removeContainerName(uri, containerName)
  }

  def removeQueryParams(url: String): String = if (StringUtils.isNotBlank(url)) url.split("\\?")(0) else url

  private def fetchFileFromUrl(url: String) = try {
    val urlPath = new URL(url)
    urlPath.getFile
  } catch {
    case e: Exception =>
      logger.error("UrlManager:getUriFromUrl:some error occurred in fetch fileName from Url:".concat(url))
      StringUtils.EMPTY
  }

  private def removeContainerName(url: String, containerName: String) = {
    val containerNameStr = "/".concat(containerName)
    logger.info("UrlManager:removeContainerName:container string formed:".concat(containerNameStr))
    url.replace(containerNameStr, "")
  }

  /**
    * getting substring from url after domainUrl/slug
    * for example for the url  domainUrl/slug/tagId/uuid.pdf then return tagId/uuid.pdf
    *
    * @param url
    * @return
    * @throws MalformedURLException
    */
  @throws[MalformedURLException]
  def getContainerRelativePath(url: String): String = if (url.startsWith("http")) {
    val uri = StringUtils.substringAfter(new URL(url).getPath, "/")
    val path = uri.split("/")
    StringUtils.join(path, "/", path.length - 2, path.length)
  }
  else url
}
