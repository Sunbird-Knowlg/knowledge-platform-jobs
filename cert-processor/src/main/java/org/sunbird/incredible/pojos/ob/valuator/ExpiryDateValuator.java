package org.sunbird.incredible.pojos.ob.valuator;

import org.sunbird.incredible.pojos.ob.exeptions.InvalidDateFormatException;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class ExpiryDateValuator implements IEvaluator {
    public String issuedDate;

    public ExpiryDateValuator(String issuedDate) {
        this.issuedDate = issuedDate;
    }

    @Override
    public String evaluates(Object inputVal) throws InvalidDateFormatException {
        return getExpiryDate((String) inputVal);
    }


    public String getExpiryDate(String expiryDate) throws InvalidDateFormatException {

        /**
         * regex of the date format yyyy-MM-dd'T'HH:mm:ss'Z'
         */
        String pattern = "\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}Z";
        if (expiryDate.matches(pattern)) {
            return expiryDate;
        } else {
            try {
                if (issuedDate == null) {
                    throw new InvalidDateFormatException("Issued date is null, please provide valid issued date ");
                } else {
                    /**
                     to split expiry dates of form (2m 2y)
                     */
                    String[] splitExpiry = expiryDate.split(" ");
                    Calendar cal = Calendar.getInstance();
                    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
                    Date parsedIssuedDate = simpleDateFormat.parse(issuedDate);
                    cal.setTime(parsedIssuedDate);

                    for (String expiry : splitExpiry) {
                        String string = checkValid(expiry);
                        switch (string.toLowerCase()) {
                            case "d":
                                cal.add(Calendar.DATE, getDigits(expiry));
                                break;
                            case "m":
                                cal.add(Calendar.MONTH, getDigits(expiry));
                                break;
                            case "y":
                                cal.add(Calendar.YEAR, getDigits(expiry));
                                break;
                            default:
                                break;
                        }
                    }
                    return simpleDateFormat.format(cal.getTime());
                }
            } catch (ParseException e) {
                return null;
            }
        }
    }

    private int getDigits(String string) {
        Pattern pattern = Pattern.compile("^\\d+");
        Matcher matcher = pattern.matcher(string);
        if (matcher.find()) {
            return Integer.parseInt(matcher.group(0));
        } else {
            return 0;
        }

    }

    private String checkValid(String string) throws InvalidDateFormatException {
        Pattern pattern = Pattern.compile("^\\d+[MYDmyd]{1}$");
        Matcher matcher = pattern.matcher(string);
        if (matcher.find()) {
            return matcher.group(0).substring(matcher.group(0).length() - 1);
        } else {
            throw new InvalidDateFormatException("Given expiry date is invalid" + string);
        }

    }
}
