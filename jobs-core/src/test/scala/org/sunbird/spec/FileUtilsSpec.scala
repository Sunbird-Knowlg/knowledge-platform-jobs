package org.sunbird.spec

import org.scalatest.{FlatSpec, Matchers}
import org.sunbird.job.util.FileUtils

import java.io.{File, FileInputStream, FileOutputStream}
import java.nio.file.{Files, Paths}
import java.util.zip.{ZipEntry, ZipOutputStream}

class FileUtilsSpec extends FlatSpec with Matchers {

  "getBasePath with empty identifier" should "return the path" in {
    val result = FileUtils.getBasePath("")
    result.nonEmpty shouldBe (true)
  }

  "downloadFile " should " download the media source file starting with http or https " in {
    val fileUrl: String = "https://preprodall.blob.core.windows.net/ntp-content-preprod/content/do_21273718766395392014320/artifact/book-image_1554832478631.jpg"
    val downloadedFile: File = FileUtils.downloadFile(fileUrl, "/tmp/contentBundle")
    assert(downloadedFile.exists())
  }

  "extractFiles " should " extract files from zip file " in {
    val zipFile = File.createTempFile("do_4444", ".zip")
    createFile(zipFile)
    val downloadedFile: List[File] = FileUtils.extractFiles(zipFile, "/tmp/contentBundle")
    assert(downloadedFile.size>0)
    Files.deleteIfExists(Paths.get("/tmp/contentBundle/do_212.txt"))
  }

  def createFile(zipFile: File): Unit = {
    val buffer = new Array[Byte](1024)
    var zos: ZipOutputStream = null
    val fout = new FileOutputStream(zipFile)
    zos = new ZipOutputStream(fout)
    val file = File.createTempFile("do_212", ".txt")
    val ze = new ZipEntry("do_212.txt")
    zos.putNextEntry(ze)
    val in = new FileInputStream(file)
    try {
      var len = in.read(buffer)
      while (len > 0) {
        zos.write(buffer, 0, len)
        len = in.read(buffer)
      }
    } finally if (in != null) in.close()
    zos.closeEntry()
    zos.flush()
    zos.close()
    fout.close()
  }
}
