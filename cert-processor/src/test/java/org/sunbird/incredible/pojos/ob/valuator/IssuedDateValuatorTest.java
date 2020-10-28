package org.sunbird.incredible.pojos.ob.valuator;

import org.sunbird.incredible.pojos.ob.exeptions.InvalidDateFormatException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import static org.junit.Assert.*;

public class IssuedDateValuatorTest {

    IssuedDateValuator issuedDateValuator = new IssuedDateValuator();
    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
    Calendar cal = Calendar.getInstance();


    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void evaluateDateInFormat1() throws InvalidDateFormatException {
        Date date = issuedDateValuator.convertToDate("2019-01-20");
        cal.setTime(date);
        assertEquals("2019-01-20T00:00:00Z", simpleDateFormat.format(cal.getTime()));
    }

    @Test
    public void evaluatesDateInFormat2() throws InvalidDateFormatException {
        Date date = issuedDateValuator.convertToDate("2019-02-12T10:11:11Z");
        cal.setTime(date);
        assertEquals("2019-02-12T10:11:11Z", simpleDateFormat.format(cal.getTime()));
    }

    @Test(expected = InvalidDateFormatException.class)
    public void evaluatesDateInNullException() throws InvalidDateFormatException {
        issuedDateValuator.convertToDate(null);
        fail("issued date cannot be null");
    }

    @Test(expected = InvalidDateFormatException.class)
    public void evaluatesIssuedDateInExceptionForDifferentFormats() throws InvalidDateFormatException {
        issuedDateValuator.convertToDate("2019-02");
        fail("issued date cannot be this format");
    }
}