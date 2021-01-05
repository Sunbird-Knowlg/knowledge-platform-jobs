package org.sunbird.publish.spec

import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.scalatestplus.mockito.MockitoSugar
import org.sunbird.publish.core.ObjectData
import org.sunbird.publish.helpers.ObjectValidator

class ObjectValidatorTestSpec extends FlatSpec with BeforeAndAfterAll with Matchers with MockitoSugar {

  override protected def beforeAll(): Unit = {
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
  }

  "Object Validator " should " validate the object and return messages" in {
    val objValidator = new TestObjectValidator()
    val obj = ObjectData("do_123", Map[String, AnyRef]("name" -> "Content Name", "identifier" -> "do_123.img", "pkgVersion" -> 2.0))
    val messages = objValidator.validate(obj, "do_123")
    messages should have length(1)
    messages should contain ("There is no mimeType defined for : do_123")
  }

}

class TestObjectValidator extends ObjectValidator {

}
