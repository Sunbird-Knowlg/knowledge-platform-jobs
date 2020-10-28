package org.sunbird.incredible.processor.qrcode;


import org.apache.commons.lang3.StringUtils;

import java.math.BigDecimal;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AccessCodeGenerator {

    private String stripChars = "0";
    private Double length = 6.0;
    private BigDecimal largePrimeNumber = new BigDecimal(1679979167);
    private String regex = "[A-Z][0-9][A-Z][0-9][A-Z][0-9]";
    private Pattern pattern = Pattern.compile(regex);

    private static final String[] ALPHABET = new String[]{"1", "2", "3", "4", "5", "6", "7", "8", "9", "A", "B", "C",
            "D", "E", "F", "G", "H", "J", "K", "L", "M", "N", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y",
            "Z"};

    public AccessCodeGenerator(Double length) {
        this.length = length;
    }

    public String generate() {
        double count = 1;
        int totalChars = ALPHABET.length;
        BigDecimal exponent = BigDecimal.valueOf(totalChars);
        exponent = exponent.pow(length.intValue());
        double codesCount = 0;
        double lastIndex = 0;
        String code = null;
        while (codesCount < count) {
            lastIndex = getMaxIndex();
            BigDecimal number = new BigDecimal(lastIndex);
            BigDecimal num = number.multiply(largePrimeNumber).remainder(exponent);
            code = baseN(num, totalChars);
            if (code.length() == length && isValidCode(code)) {
                codesCount += 1;
            }
        }
        return code;

    }

    private String baseN(BigDecimal num, int base) {
        if (num.doubleValue() == 0) {
            return "0";
        }
        double div = Math.floor(num.doubleValue() / base);
        String val = baseN(new BigDecimal(div), base);
        return StringUtils.stripStart(val, stripChars) + ALPHABET[num.remainder(new BigDecimal(base)).intValue()];
    }


    private Double getMaxIndex() {
        return (double) System.currentTimeMillis();
    }

    /**
     * This Method will check if dialcode has numeric value at odd indexes.
     *
     * @param code
     * @return Boolean
     */
    private Boolean isValidCode(String code) {
        Matcher matcher = pattern.matcher(code);
        return matcher.matches();
    }


}
