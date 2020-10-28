package org.sunbird.incredible.pojos.ob.valuator;

import org.sunbird.incredible.pojos.ob.exeptions.InvalidDateFormatException;

public interface IEvaluator {

    String evaluates(Object inputVal) throws InvalidDateFormatException;
}
