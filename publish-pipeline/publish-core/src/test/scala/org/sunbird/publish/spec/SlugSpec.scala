package org.sunbird.publish.spec

import java.io.File

import org.scalatest.{FlatSpec, Matchers}
import org.sunbird.publish.core.Slug

class SlugSpec extends FlatSpec with Matchers {

	"test makeSlug" should "return make slug successfully" in {
		val sluggified = Slug.makeSlug(" -Cov -e*r+I/ αma.ge.png-- ")
		assert("cov-er-i-ma.ge.png" == sluggified)
	}
	"test makeSlug with null" should "throw IllegalArgumentException" in {
		intercept[IllegalArgumentException] {
			Slug.makeSlug(null)
		}
	}
	"test makeSlug with Transliterate" should "throw IllegalArgumentException" in {
		val sluggified = Slug.makeSlug(" Cov -e*r+I/ αma.ge.png ", true)
		assert("cov-er-i-ama.ge.png" == sluggified)
	}
	"test makeSlug with duplicates" should "throw IllegalArgumentException" in {
		val sluggified = Slug.removeDuplicateChars("akssaaklla")
		assert("aksakla" == sluggified)
	}
	"test create Slug file" should "throw IllegalArgumentException" in {
		val file = new File("-αimage.jpg")
		val slugFile = Slug.createSlugFile(file)
		assert("aimage.jpg" == slugFile.getName)
	}
}
