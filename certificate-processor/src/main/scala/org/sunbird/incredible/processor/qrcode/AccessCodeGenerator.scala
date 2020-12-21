package org.sunbird.incredible.processor.qrcode


import scala.math.BigDecimal
import org.apache.commons.lang3.StringUtils

import scala.util.matching.Regex

class AccessCodeGenerator(length: Double = 6.0) {
  private val stripChars: String = "0"
  private val largePrimeNumber = BigDecimal(1679979167)
  private val regex: Regex = "[A-Z][0-9][A-Z][0-9][A-Z][0-9]".r
  private val pattern: java.util.regex.Pattern = regex.pattern

  private val ALPHABET = Array[String]("1", "2", "3", "4", "5", "6", "7", "8", "9", "A", "B", "C", "D", "E", "F", "G", "H", "J", "K", "L", "M", "N", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z")


  def generate(): String = {
    val count: Double = 1
    val totalChars: Int = ALPHABET.length
    var exponent: BigDecimal = BigDecimal.valueOf(totalChars)
    exponent = exponent.pow(length.intValue)
    var codesCount: Double = 0
    var lastIndex: Double = 0
    var code: String = null
    while (codesCount < count) {
      lastIndex = getMaxIndex
      val number: BigDecimal = BigDecimal(lastIndex)
      val num: BigDecimal = number.*(largePrimeNumber).remainder(exponent)
      code = baseN(num, totalChars)
      if (code.length == length && isValidCode(code))
        codesCount += 1
    }
    code
  }

  private def baseN(num: BigDecimal, base: Int): String = {
    if (num.doubleValue == 0) return "0"
    val div = Math.floor(num.doubleValue / base)
    val value = baseN(BigDecimal(div), base)
    StringUtils.stripStart(value, stripChars) + ALPHABET(num.remainder(BigDecimal(base)).intValue)
  }


  private def getMaxIndex = System.currentTimeMillis.toDouble

  /**
    * This Method will check if dialcode has numeric value at odd indexes.
    *
    * @param code
    * @return Boolean
    */
  private def isValidCode(code: String): Boolean = {
    val matcher = pattern.matcher(code)
    matcher.matches()
  }


}
