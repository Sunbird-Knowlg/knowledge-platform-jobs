package org.sunbird.incredible.processor.views

import java.io.IOException
import java.util.regex.Matcher

import com.twitter.storehaus.cache.{Cache, LRUCache}
import org.apache.commons.lang.StringUtils
import org.apache.commons.text.StringSubstitutor
import org.slf4j.{Logger, LoggerFactory}
import org.sunbird.incredible.pojos.ob.CertificateExtension

import scala.io.{BufferedSource, Source}
import scala.util.matching.Regex

object EncoderMap {
  val encoder: Map[String, String] = Map("<" -> "%3C", ">" -> "%3E", "#" -> "%23", "%" -> "%25", "\"" -> "\'")
}


object SvgGenerator {
  private val logger: Logger = LoggerFactory.getLogger(getClass.getName)
  private var svgTemplatesCache: Cache[String, String] = LRUCache[String, String](15)

  @throws[IOException]
  def generate(certificateExtension: CertificateExtension, encodedQrCode: String, svgTemplateUrl: String): String = {
    var cachedTemplate = svgTemplatesCache.get(svgTemplateUrl).getOrElse("")
    if (StringUtils.isEmpty(cachedTemplate)) {
      logger.info("{} svg not cached , downloading", svgTemplateUrl)
      cachedTemplate = download(svgTemplateUrl)
      cachedTemplate = "data:image/svg+xml," + encodeData(cachedTemplate)
      cachedTemplate = cachedTemplate.replaceAll("\n", "").replaceAll("\t", "")
      svgTemplatesCache = svgTemplatesCache.put(svgTemplateUrl, cachedTemplate)._2
    } else {
      svgTemplatesCache = svgTemplatesCache.hit(svgTemplateUrl)
    }
    val svgData = replaceTemplateVars(cachedTemplate, certificateExtension, encodedQrCode)
    logger.info("svg template string creation completed")
    svgData
  }


  private def replaceTemplateVars(svgContent: String, certificateExtension: CertificateExtension, encodeQrCode: String): String = {
    val varResolver = new VarResolver(certificateExtension)
    val certData: java.util.Map[String, String] = varResolver.getCertMetaData
    certData.put("qrCodeImage", "data:image/png;base64," + encodeQrCode)
    println(certData)
    val sub = new StringSubstitutor(certData)
    val resolvedString = sub.replace(svgContent)
    logger.info("replacing temp vars completed")
    resolvedString
  }

  private def encodeData(data: String): String = {
    val stringBuffer: StringBuffer = new StringBuffer
    val regex: Regex = "[<>#%\"]".r
    val pattern: java.util.regex.Pattern = regex.pattern
    val matcher: Matcher = pattern.matcher(data)
    while (matcher.find)
      matcher.appendReplacement(stringBuffer, EncoderMap.encoder(matcher.group))
    matcher.appendTail(stringBuffer)
    stringBuffer.toString
  }

  @throws[IOException]
  private def download(svgTemplate: String): String = {
    var svgData: BufferedSource = null
    try {
      svgData = Source.fromURL(svgTemplate)
    } finally {
      svgData.close
    }
    svgData.mkString
  }

}
