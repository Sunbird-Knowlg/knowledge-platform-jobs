package org.sunbird.incredible.pojos.valuator

import org.sunbird.incredible.pojos.exceptions.InvalidDateFormatException

trait IEvaluator {
  @throws[InvalidDateFormatException]
  def evaluates(inputVal: AnyRef): String
}
