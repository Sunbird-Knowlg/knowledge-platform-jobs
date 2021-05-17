package org.sunbird.job.publish.spec

import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.scalatestplus.mockito.MockitoSugar
import org.sunbird.job.publish.core.ObjectData
import org.sunbird.job.publish.helpers.ObjectValidator

class ObjectValidatorTestSpec extends FlatSpec with BeforeAndAfterAll with Matchers with MockitoSugar {

  override protected def beforeAll(): Unit = {
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
  }

  "Object Validator " should " validate the object and return messages" in {
    val objValidator = new TestObjectValidator()
    val obj = new ObjectData("do_123", Map[String, AnyRef]("name" -> "Content Name", "identifier" -> "do_123.img", "IL_UNIQUE_ID" -> "do_123.img", "pkgVersion" -> 2.0.asInstanceOf[AnyRef]))
    val messages = objValidator.validate(obj, "do_123")
    messages should have length(3)
    messages should contain ("There is no mimeType defined for : do_123")
    messages should contain ("There is no primaryCategory defined for : do_123")
    messages should contain ("There is no code defined for : do_123")
  }

}

class TestObjectValidator extends ObjectValidator {

}
