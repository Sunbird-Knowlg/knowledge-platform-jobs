package org.sunbird.incredible.processor.views;

import java.util.ArrayList;
import java.util.List;

public class HTMLVars {
    private static List<String> allVars = new ArrayList<>();
    static {
        for (SupportedVars htmlVars : SupportedVars.values()) {
            allVars.add(htmlVars.toString());
        }
    }

    public static List<String> get() {
        return allVars;
    }

    public enum SupportedVars {
        //badge class
        $certificateName,
        $certificateDescription,
        //CompositeIdentityObject - profile
        $recipientName,
        $recipientId,
        //Assertion class
        $issuedDate,
        $expiryDate,
        //Signatory class
        $signatory0Image,
        $signatory0Designation,
        $signatory1Image,
        $signatory1Designation,
        //others
        $courseName,
        $qrCodeImage,
        $issuerName
    }
}

