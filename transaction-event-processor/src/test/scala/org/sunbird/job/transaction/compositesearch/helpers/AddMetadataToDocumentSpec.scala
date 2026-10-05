package org.sunbird.job.transaction.compositesearch.helpers

import org.sunbird.spec.BaseTestSpec

import java.util

/**
 * Fields declared nested reach the indexer either as a JSON string or already as an
 * object; both must end up as the object the nested mapping expects. Everything else
 * is passed through as it arrives.
 */
class AddMetadataToDocumentSpec extends BaseTestSpec {

  private val helper = new CompositeSearchIndexerHelper {}

  private val nested = List("trackable", "credentials", "discussionForum", "batches", "plugins", "transcoding")

  private def shape(name: String, value: AnyRef): AnyRef =
    helper.addMetadataToDocument(name, value, nested)

  private def transcodingObject = new util.HashMap[String, AnyRef]() {
    put("status", "STARTED")
    put("retryCount", Integer.valueOf(0))
  }

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

  /** The regression: a video upload sends transcoding as an object. */
  "transcoding" should "be passed on as an object when it arrives as one" in {
    val value = transcodingObject
    shape("transcoding", value) should be(value)
  }

  it should "be parsed into an object when it arrives as a JSON string" in {
    val result = shape("transcoding", """{"status":"STARTED","retryCount":0}""")
    result.isInstanceOf[String] should be(false)
  }

  "a field that is not declared nested" should "be passed through as it arrives, object or not" in {
    val value = transcodingObject
    helper.addMetadataToDocument("transcoding", value, List("trackable")) should be(value)
    shape("name", "A course") should be("A course")
    shape("pkgVersion", Integer.valueOf(3)) should be(Integer.valueOf(3))
    shape("userConsent", java.lang.Boolean.TRUE) should be(java.lang.Boolean.TRUE)

    val languages = util.Arrays.asList("English", "Hindi")
    shape("language", languages) should be(languages)
  }
}
