package org.sunbird.job.spec

import org.sunbird.job.assetenricment.util.Slug
import org.sunbird.spec.BaseTestSpec

import java.io.File

class SlugSpec extends BaseTestSpec{

  "createSlugFile " should " create slug file for the provided file" in {
    val file = new File("-αimage.jpg")
    val slugFile = Slug.createSlugFile(file)
    assert("aimage.jpg" == slugFile.getName)
  }

  "createSlugFile " should " return the original file" in {
    val file: File = null
    val slugFile = Slug.createSlugFile(file)
    assert(slugFile == null)
  }

  "makeSlug " should " return the original file" in {
    val slug = Slug.makeSlug("do_113233717480390656195", false)
    assert(slug == "do_113233717480390656195")
  }

  "makeSlug " should " return error" in {
    assertThrows[Exception] {
      Slug.makeSlug(null, false)
    }
  }

  "urlDecode " should " return empty string for null input" in {
    val slug = Slug.urlDecode(null)
    assert(slug == "")
  }

  "validateResult " should " throw exception string for null input" in {
    assertThrows[Exception] {
      Slug.validateResult("", "test")
    }
  }

}
