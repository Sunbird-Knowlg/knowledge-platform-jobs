package org.sunbird.incredible.valuator

import org.junit.Assert.assertEquals
import org.sunbird.incredible.BaseTestSpec
import org.sunbird.incredible.pojos.valuator.ExpiryDateValuator

class ExpiryDateValuatorTest  extends BaseTestSpec {

  private var issuedDate: String = "2019-08-31T12:52:25Z"


  "check expiry Date Is correct For Months" should "should return valid expiry date for months" in {
    val expiryDate = "2m"
    val expiryDateValuator = new ExpiryDateValuator(issuedDate)
    val expiry = expiryDateValuator.evaluates(expiryDate)
    assertEquals("expiry date is valid for months ", "2019-10-31T12:52:25Z", expiry)
  }

  "check expiry Date Is correct For years" should "should return valid expiry date for years" in {
    val expiryDate = "2Y"
    val expiryDateValuator = new ExpiryDateValuator(issuedDate)
    val expiry = expiryDateValuator.evaluates(expiryDate)
    assertEquals("expiry date is valid for years", "2021-08-31T12:52:25Z", expiry)
  }

  "check expiry Date Is correct For days" should "should return valid expiry date for days" in {
    val expiryDate = "2d"
    val expiryDateValuator = new ExpiryDateValuator(issuedDate)
    val expiry = expiryDateValuator.evaluates(expiryDate)
    assertEquals("expiry date is valid for days", "2019-09-02T12:52:25Z", expiry)
  }

  "check expiry date is correct For days and years" should "should return valid expiry date for both days and years" in {
    val expiryDate = "2D 2y"
    val expiryDateValuator = new ExpiryDateValuator(issuedDate)
    val expiry = expiryDateValuator.evaluates(expiryDate)
    assertEquals("expiry date is valid for both days and years", "2021-09-02T12:52:25Z", expiry)
  }

  "check expiry date is correct For both months and years" should "should return valid expiry date for both months and years" in {
    val expiryDate = "2M 1y"
    val expiryDateValuator = new ExpiryDateValuator(issuedDate)
    val expiry = expiryDateValuator.evaluates(expiryDate)
    assertEquals("expiry date is valid for both months and years", "2020-10-31T12:52:25Z", expiry)
  }

  "check expiry date is correct For both both days and months" should "should return valid expiry date for both days and months" in {
    val expiryDate = "2d 2m"
    val expiryDateValuator = new ExpiryDateValuator(issuedDate)
    val expiry = expiryDateValuator.evaluates(expiryDate)
    assertEquals("expiry date is valid for both days and months", "2019-11-02T12:52:25Z", expiry)
  }
  "check expiry date is correct in format" should "return valid expiry date" in {
    val expiryDate = "2019-09-02T12:52:25Z"
    val expiryDateValuator = new ExpiryDateValuator(issuedDate)
    val expiry = expiryDateValuator.evaluates(expiryDate)
    assertEquals("expiry date is incorrect format", "2019-09-02T12:52:25Z", expiry)
  }

}
