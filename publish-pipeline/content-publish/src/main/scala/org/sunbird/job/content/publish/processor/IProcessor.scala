package org.sunbird.job.content.publish.processor

import org.sunbird.job.publish.util.CloudStorageUtil

abstract class IProcessor(basePath: String, identifier: String)(implicit cloudStorageUtil: CloudStorageUtil) {

    implicit val ss = cloudStorageUtil.getService

    val widgetTypeAssets:List[String] = List("js", "css", "json", "plugin")
    val whiteListedMimeTypes: List[String] = List("application/vnd.ekstep.ecml-archive","application/vnd.ekstep.html-archive","application/vnd.android.package-archive","application/vnd.ekstep.content-archive","application/vnd.ekstep.content-collection","application/octet-stream","application/json","application/javascript","application/xml","text/plain","text/html","text/javascript","text/xml","text/css","image/jpeg","image/jpg","image/png","image/tiff","image/bmp","image/gif","image/svg+xml","image/x-quicktime","image/x-quicktime","image/x-quicktime","video/avi","video/avi","video/msvideo","video/x-msvideo","video/mpeg","video/quicktime","video/quicktime","video/x-qtc","video/3gpp","video/mp4","video/ogg","video/webm","video/mpeg","video/x-mpeg","audio/mp3","audio/mpeg3","audio/x-mpeg-3","audio/mp4","audio/mpeg","audio/ogg","audio/vorbis","audio/webm","audio/x-wav","application/x-font-ttf")
    val blackListedMimeTypes: List[String] = List()
    def process(ecrf: Plugin): Plugin

    def getBasePath():String = basePath
    def getIdentifier():String = identifier
}