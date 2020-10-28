package org.sunbird.incredible.pojos.ob.valuator;

import org.sunbird.incredible.pojos.ob.exeptions.InvalidDateFormatException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;


public class ExpiryDateValuatorTest {
    private String issuedDate;


    @Before
    public void setUp() throws Exception {
        issuedDate = "2019-08-31T12:52:25Z";
    }

    @After
    public void tearDown() throws Exception {

    }


    @Test
    public void expiryDateIsCorrectForMonths() throws InvalidDateFormatException {
        String expiryDate = "2m";
        ExpiryDateValuator expiryDateValuator = new ExpiryDateValuator(issuedDate);
        String expiry = expiryDateValuator.getExpiryDate(expiryDate);
        assertEquals("expiry date is valid for months ","2019-10-31T12:52:25Z", expiry);
    }

    @Test
    public void expiryDateIsCorrectForYears() throws InvalidDateFormatException {
        String expiryDate = "2Y";
        ExpiryDateValuator expiryDateValuator = new ExpiryDateValuator(issuedDate);
        String expiry = expiryDateValuator.getExpiryDate(expiryDate);
        assertEquals("expiry date is valid for years","2021-08-31T12:52:25Z", expiry);
    }

    @Test
    public void expiryDateIsCorrectForDays() {
        String expiryDate = "2d";
        ExpiryDateValuator expiryDateValuator = new ExpiryDateValuator(issuedDate);
        String expiry = null;
        try {
            expiry = expiryDateValuator.getExpiryDate(expiryDate);
        } catch (InvalidDateFormatException invalidDateFormatException) {

        }
        assertEquals("expiry date is valid for days","2019-09-02T12:52:25Z", expiry);
    }

    @Test
    public void expiryDateIsCorrectForDaysAndYears() {
        String expiryDate = "2D 2y";
        ExpiryDateValuator expiryDateValuator = new ExpiryDateValuator(issuedDate);
        String expiry = null;
        try {
            expiry = expiryDateValuator.getExpiryDate(expiryDate);
        } catch (InvalidDateFormatException invalidDateFormatException) {
        }
        assertEquals("expiry date is valid for both days and years","2021-09-02T12:52:25Z", expiry);
    }

    @Test
    public void expiryDateIsCorrectForMonthsAndYears() {
        String expiryDate = "2M 1y";
        ExpiryDateValuator expiryDateValuator = new ExpiryDateValuator(issuedDate);
        String expiry = null;
        try {
            expiry = expiryDateValuator.getExpiryDate(expiryDate);
        } catch (InvalidDateFormatException invalidDateFormatException) {
        }
        assertEquals("expiry date is valid for both months and years","2020-10-31T12:52:25Z", expiry);
    }

    @Test
    public void expiryDateIsCorrectForDaysAndMonths() {
        String expiryDate = "2d 2m";
        ExpiryDateValuator expiryDateValuator = new ExpiryDateValuator(issuedDate);
        String expiry = null;
        try {
            expiry = expiryDateValuator.getExpiryDate(expiryDate);
        } catch (InvalidDateFormatException invalidDateFormatException) {
        }
        assertEquals("expiry date is valid for both days and months","2019-11-02T12:52:25Z", expiry);
    }

    @Test
    public void expiryDateIsCorrectInFormat() {
        String expiryDate = "2019-09-02T12:52:25Z";
        ExpiryDateValuator expiryDateValuator = new ExpiryDateValuator(issuedDate);
        String expiry = null;
        try {
            expiry = expiryDateValuator.getExpiryDate(expiryDate);
        } catch (InvalidDateFormatException invalidDateFormatException) {
        }
        assertEquals("expiry date is incorrect format","2019-09-02T12:52:25Z", expiry);
    }

    @Test(expected = InvalidDateFormatException.class)
    public void expiryDateIsNullCausesException() throws InvalidDateFormatException {
        String expiryDate = "2019-34-12";
        ExpiryDateValuator expiryDateValuator = new ExpiryDateValuator(issuedDate);
        expiryDateValuator.getExpiryDate(expiryDate);
        fail("No exception thrown for null expiry date");
    }


}