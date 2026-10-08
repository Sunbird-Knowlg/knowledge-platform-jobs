package org.sunbird.job.transaction.compositesearch.helpers

import org.sunbird.spec.BaseTestSpec

import java.util

/**
 * Every value must reach OpenSearch in the shape its mapping accepts: fields declared
 * nested as objects, everything else -- mapped as text -- as a string. A value in the
 * wrong shape is rejected with a mapper_parsing_exception, the document is never
 * indexed, and the indexing job stops on it.
 */
class AddMetadataToDocumentSpec extends BaseTestSpec {

  private val helper = new CompositeSearchIndexerHelper {}

  private val nested = List("trackable", "credentials", "batches", "plugins", "transcoding")

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

  /** A video upload sends transcoding as an object. */
  "transcoding" should "be passed on as an object when it arrives as one" in {
    val value = transcodingObject
    shape("transcoding", value) should be(value)
  }

  it should "be parsed into an object when it arrives as a JSON string" in {
    val result = shape("transcoding", """{"status":"STARTED","retryCount":0}""")
    result.isInstanceOf[String] should be(false)
  }

  /** The regression: a question's choices arrived as an object against a text mapping. */
  "an object on a field that is not declared nested" should "be serialised to a JSON string" in {
    val choices = new util.HashMap[String, AnyRef]() { put("options", util.Arrays.asList("A", "B")) }
    val result = shape("choices", choices)
    result.isInstanceOf[String] should be(true)
    result.asInstanceOf[String] should include("options")
  }

  it should "be serialised when it is a Scala map too" in {
    val result = shape("discussionForum", Map("enabled" -> "No"))
    result should be("""{"enabled":"No"}""")
  }

  it should "be serialised when a field is not listed as nested in this environment" in {
    val result = helper.addMetadataToDocument("transcoding", transcodingObject, List("trackable"))
    result.isInstanceOf[String] should be(true)
  }

  "a list holding an object on a field that is not declared nested" should "be serialised to a JSON string" in {
    val javaList = util.Arrays.asList[AnyRef](new util.HashMap[String, AnyRef]() { put("id", "q1") })
    shape("videoQuestions", javaList).isInstanceOf[String] should be(true)

    val scalaList = List(Map("id" -> "q1"))
    shape("videoQuestions", scalaList).isInstanceOf[String] should be(true)
  }

  "scalar values and lists of scalars" should "pass through untouched" in {
    shape("name", "A course") should be("A course")
    shape("pkgVersion", Integer.valueOf(3)) should be(Integer.valueOf(3))
    shape("userConsent", java.lang.Boolean.TRUE) should be(java.lang.Boolean.TRUE)

    val languages = util.Arrays.asList("English", "Hindi")
    shape("language", languages) should be(languages)

    val scalaLanguages = List("English", "Hindi")
    shape("language", scalaLanguages) should be(scalaLanguages)
  }

  "an empty list" should "pass through untouched" in {
    val empty = new util.ArrayList[AnyRef]()
    shape("videoQuestions", empty) should be(empty)
  }
}
