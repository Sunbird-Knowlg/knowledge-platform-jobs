package org.sunbird.job.spec

import java.io.File

import org.sunbird.job.util.AssetFileUtils
import org.sunbird.spec.BaseTestSpec

class AssetFileUtilsSpec extends BaseTestSpec{

  "getBasePath" should "return empty string for empty Object ID" in {
    val path = AssetFileUtils.getBasePath("")
    path.isEmpty should be(true)
  }

  "getFileType" should "return file type for the provided audio file" in {
    val file = new File(getClass.getClassLoader.getResource("test_files/test_audio.mp3").getFile)
    val fileType = AssetFileUtils.getFileType(file)
    assert(fileType == "Audio")
  }

  "getFileType" should "return file type for the provided video file" in {
    val file = new File(getClass.getClassLoader.getResource("test_files/test_video.mp4").getFile)
    val fileType = AssetFileUtils.getFileType(file)
    assert(fileType == "Video")
  }

  "getFileType" should "return file type for the provided pdf file" in {
    val file = new File(getClass.getClassLoader.getResource("test_files/test_other.pdf").getFile)
    val fileType = AssetFileUtils.getFileType(file)
    assert(fileType == "Other")
  }

  "getFileType" should "return file type for the provided directory" in {
    val file = new File(getClass.getClassLoader.getResource("test_files").getFile)
    val fileType = AssetFileUtils.getFileType(file)
    assert(fileType == "Directory")
  }

}
