package org.sunbird.job.transaction.compositesearch.helpers

import org.sunbird.spec.BaseTestSpec

import java.util

/**
 * The composite search mapping types every field that is not declared an object as
 * text, so an object reaching one of those fields is rejected outright and the whole
 * document goes unindexed. These cover the shape each kind of value is given.
 */
class AddMetadataToDocumentSpec extends BaseTestSpec {

  private val helper = new CompositeSearchIndexerHelper {}

  private val nested = List("trackable", "credentials", "discussionForum", "batches", "plugins")

  private def shape(name: String, value: AnyRef): AnyRef =
    helper.addMetadataToDocument(name, value, nested)

  "a field declared nested" should "be parsed from its JSON string into an object" in {
    val result = shape("trackable", """{"enabled":"Yes","autoBatch":"Yes"}""")
    result.isInstanceOf[String] should be(false)
  }

  it should "be left alone when it already arrives as an object" in {
    val value = new util.HashMap[String, AnyRef]() { put("enabled", "Yes") }
    shape("credentials", value) should be(value)
  }

  it should "fall back to the raw string when it is not valid JSON" in {
    shape("plugins", "not-json") should be("not-json")
  }

  /** The regression: transcoding arrived as an object against a text mapping. */
  "an object on a field that is not declared nested" should "be serialised to a JSON string" in {
    val value = new util.HashMap[String, AnyRef]() {
      put("status", "STARTED")
      put("retryCount", Integer.valueOf(0))
    }

    val result = shape("transcoding", value)

    result.isInstanceOf[String] should be(true)
    result.asInstanceOf[String] should include("STARTED")
  }

  "scalar and array values" should "pass through untouched" in {
    shape("name", "A course") should be("A course")
    shape("pkgVersion", Integer.valueOf(3)) should be(Integer.valueOf(3))
    shape("userConsent", java.lang.Boolean.TRUE) should be(java.lang.Boolean.TRUE)

    val languages = util.Arrays.asList("English", "Hindi")
    shape("language", languages) should be(languages)
  }
}
