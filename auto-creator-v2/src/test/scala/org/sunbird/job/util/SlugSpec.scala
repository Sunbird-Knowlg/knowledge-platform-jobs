package org.sunbird.job.util

import java.io.File

import org.scalatest.{FlatSpec, Matchers}

class SlugSpec extends FlatSpec with Matchers {

	"test makeSlug" should "return make slug successfully" in {
		val sluggified = Slug.makeSlug(" -Cov -e*r+I/ αma.ge.png-- ", false)
		assert("cov-er-i-ma.ge.png" == sluggified)
	}
	"test makeSlug with null" should "throw IllegalArgumentException" in {
		intercept[IllegalArgumentException] {
			Slug.makeSlug(null, false)
		}
	}
	"test makeSlug with Transliterate" should "throw IllegalArgumentException" in {
		val sluggified = Slug.makeSlug(" Cov -e*r+I/ αma.ge.png ", true)
		assert("cov-er-i-ama.ge.png" == sluggified)
	}

	"test create Slug file" should "throw IllegalArgumentException" in {
		val file = new File("-αimage.jpg")
		val slugFile = Slug.createSlugFile(file)
		assert("aimage.jpg" == slugFile.getName)
	}
}
