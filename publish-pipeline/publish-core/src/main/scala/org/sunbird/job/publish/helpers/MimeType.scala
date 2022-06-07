package org.sunbird.job.publish.helpers

object MimeType extends Enumeration {

  val PDF: String = Value("application/pdf").toString
  val EPUB: String = Value("application/epub").toString
  val MSWORD: String = Value("application/msword").toString
  val H5P_Archive: String = Value("application/vnd.ekstep.h5p-archive").toString

  val X_Youtube: String = Value("video/x-youtube").toString
  val Youtube: String = Value("video/youtube").toString
  val X_URL: String = Value("text/x-url").toString

  val ECML_Archive: String = Value("application/vnd.ekstep.ecml-archive").toString
  val HTML_Archive: String = Value("application/vnd.ekstep.html-archive").toString
  val Android_Package: String = Value("application/vnd.android.package-archive").toString
  val Content_Archive: String = Value("application/vnd.ekstep.content-archive").toString

  val ASSETS: String = Value("assets").toString
  val Plugin_Archive: String = Value("application/vnd.ekstep.plugin-archive").toString
  val Collection: String = Value("application/vnd.ekstep.content-collection").toString

  val Content_Package: String = Value("application/octet-stream").toString
  val JSON_FILE: String = Value("application/json").toString
  val JS_FILE: String = Value("application/javascript").toString
  val XML_FILE: String = Value("application/xml").toString
  val TEXT_FILE: String = Value("text/plain").toString
  val HTML_FILE: String = Value("text/html").toString
  val TEXT_JS_FILE: String = Value("text/javascript").toString
  val TEXT_XML_FILE: String = Value("text/xml").toString
  val CSS_FILE: String = Value("text/css").toString
  val JPEG_Image: String = Value("image/jpeg").toString
  val JPG_Image: String = Value("image/jpg").toString
  val PNG_Image: String = Value("image/png").toString
  val TIFF_Image: String = Value("image/tiff").toString
  val BMP_Image: String = Value("image/bmp").toString
  val GIF_Image: String = Value("image/gif").toString
  val SVG_XML_Image: String = Value("image/svg+xml").toString
  val QUICK_TIME_Image: String = Value("image/x-quicktime").toString
  val AVI_Video: String = Value("video/avi").toString
  val AVI_MS_VIDEO_Video: String = Value("video/msvideo").toString
  val AVI_X_MS_VIDEO_Video: String = Value("video/x-msvideo").toString
  val QUICK_TIME_Video: String = Value("video/quicktime").toString
  val X_QUICK_TIME_Video: String = Value("video/x-qtc").toString
  val III_gpp_Video: String = Value("video/3gpp").toString
  val MP4_Video: String = Value("video/mp4").toString
  val OGG_Video: String = Value("video/ogg").toString
  val WEBM_Video: String = Value("video/webm").toString
  val MP3_MPEG_Audio: String = Value("video/mpeg").toString
  val MP3_X_MPEG_Audio: String = Value("video/x-mpeg").toString
  val MP3_Audio: String = Value("audio/mp3").toString
  val MP3_MPEG3_Audio: String = Value("audio/mpeg3").toString
  val MP3_X_MPEG3_Audio: String = Value("audio/x-mpeg-3").toString
  val MP4_Audio: String = Value("audio/mp4").toString
  val MPEG_Audio: String = Value("audio/mpeg").toString
  val OGG_Audio: String = Value("audio/ogg").toString
  val OGG_VORBIS_Audio: String = Value("audio/vorbis").toString
  val WEBM_Audio: String = Value("audio/webm").toString
  val X_WAV_Audio: String = Value("audio/x-wav").toString
  val X_FONT_TTF: String = Value("application/x-font-ttf").toString
}
