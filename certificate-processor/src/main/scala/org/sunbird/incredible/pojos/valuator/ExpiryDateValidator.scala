package org.sunbird.incredible.pojos.valuator

import java.text.{ParseException, SimpleDateFormat}
import java.util.regex.{Matcher, Pattern}
import java.util.{Calendar, Date}

import org.sunbird.incredible.pojos.exceptions.InvalidDateFormatException

class ExpiryDateValidator(var issuedDate: String) extends IEvaluator {

  override def evaluates(inputVal: AnyRef): String =
    getExpiryDate(inputVal.asInstanceOf[String])

  def getExpiryDate(expiryDate: String): String = {
    /**
      * regex of the date format yyyy-MM-dd'T'HH:mm:ss'Z'
      */
    val pattern: String = "\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}Z"
    if (expiryDate.matches(pattern)) {
      expiryDate
    } else {
      try if (issuedDate == null) {
        throw new InvalidDateFormatException("Issued date is null, please provide valid issued date ")
      } else {

        /**
          * to split expiry dates of form (2m 2y)
          */
        val splitExpiry: Array[String] = expiryDate.split(" ")
        val cal: Calendar = Calendar.getInstance
        val simpleDateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")
        val parsedIssuedDate: Date = simpleDateFormat.parse(issuedDate)
        cal.setTime(parsedIssuedDate)
        for (expiry <- splitExpiry) {
          val string: String = checkValid(expiry)
          string.toLowerCase() match {
            case "d" => cal.add(Calendar.DATE, getDigits(expiry))
            case "m" => cal.add(Calendar.MONTH, getDigits(expiry))
            case "y" => cal.add(Calendar.YEAR, getDigits(expiry))
            case _ => //break

          }
        }
        simpleDateFormat.format(cal.getTime)
      } catch {
        case e: ParseException => null

      }
    }
  }

  private def getDigits(string: String): Int = {
    val pattern: Pattern = Pattern.compile("^\\d+")
    val matcher: Matcher = pattern.matcher(string)
    if (matcher.find()) {
      java.lang.Integer.parseInt(matcher.group(0))
    } else {
      0
    }
  }

  private def checkValid(string: String): String = {
    val pattern: Pattern = Pattern.compile("^\\d+[MYDmyd]{1}$")
    val matcher: Matcher = pattern.matcher(string)
    if (matcher.find()) {
      matcher.group(0).substring(matcher.group(0).length - 1)
    } else {
      throw new InvalidDateFormatException(
        "Given expiry date is invalid" + string)
    }
  }

}

