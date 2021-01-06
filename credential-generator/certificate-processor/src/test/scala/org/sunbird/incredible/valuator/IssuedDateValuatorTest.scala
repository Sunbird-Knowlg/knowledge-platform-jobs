package org.sunbird.incredible.valuator

import java.text.SimpleDateFormat
import java.util.Calendar

import org.junit.Assert.assertEquals
import org.sunbird.incredible.BaseTestSpec
import org.sunbird.incredible.pojos.exceptions.InvalidDateFormatException
import org.sunbird.incredible.pojos.valuator.IssuedDateValuator

class IssuedDateValuatorTest extends BaseTestSpec {


  private val issuedDateValuator = new IssuedDateValuator
  private val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")
  private val cal = Calendar.getInstance


  "evaluate date in format1" should "should parse date in correct format" in {
    val date = issuedDateValuator.convertToDate("2019-01-20")
    cal.setTime(date)
    assertEquals("2019-01-20T00:00:00Z", simpleDateFormat.format(cal.getTime))

  }

  "evaluate date in format2" should "should parse date in correct format" in {
    val date = issuedDateValuator.convertToDate("2019-02-12T10:11:11Z")
    cal.setTime(date)
    assertEquals("2019-02-12T10:11:11Z", simpleDateFormat.format(cal.getTime))

  }


  "evaluate date which has null value" should "should throw exception" in {
    intercept[InvalidDateFormatException] {
      issuedDateValuator.convertToDate(null)
    }
  }


  "evaluate date For different formats " should "should throw exception" in {
    intercept[InvalidDateFormatException] {
      issuedDateValuator.convertToDate("2019-02")
    }
  }


}
