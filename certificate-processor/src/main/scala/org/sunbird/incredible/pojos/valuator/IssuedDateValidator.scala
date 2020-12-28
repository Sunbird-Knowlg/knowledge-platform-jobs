package org.sunbird.incredible.pojos.valuator

import java.text.{ParseException, SimpleDateFormat}
import java.util.{Calendar, Date}

import org.apache.commons.lang.StringUtils
import org.sunbird.incredible.pojos.exceptions.InvalidDateFormatException

import scala.util.control.Breaks

class IssuedDateValidator extends IEvaluator {
  private val dateFormats = List(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'"), new SimpleDateFormat("yyyy-MM-dd"))
  private val loop = new Breaks

  @throws[InvalidDateFormatException]
  override def evaluates(inputVal: AnyRef): String = {
    val simpleDateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")
    val cal: Calendar = Calendar.getInstance
    val date: Date = convertToDate(inputVal.asInstanceOf[String])
    cal.setTime(date)
    simpleDateFormat.format(cal.getTime)
  }

  def convertToDate(input: String): Date = {
    var date: Date = null
    if (StringUtils.isEmpty(input)) {
      throw new InvalidDateFormatException("issued date cannot be null")
    }
    loop.breakable {
      dateFormats.foreach(format => {
        try {
          format.setLenient(false)
          date = format.parse(input)
        } catch {
          case e: ParseException =>
        }
        if (date != null) {
          loop.break
        }
      })
    }
    if (date == null) {
      throw new InvalidDateFormatException("issued date is not in valid format")
    } else {
      date
    }
  }
}
