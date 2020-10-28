package org.sunbird.incredible.processor.views;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.sunbird.incredible.processor.JsonKey;
import org.sunbird.incredible.pojos.CertificateExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

public class HTMLVarResolver {

    private CertificateExtension certificateExtension;

    private static Logger logger = LoggerFactory.getLogger(HTMLVarResolver.class);

    public HTMLVarResolver(CertificateExtension certificateExtension) {
        this.certificateExtension = certificateExtension;
    }



    public String getRecipientName() {
        return certificateExtension.getRecipient().getName();
    }

    public String getRecipientId() {
        return certificateExtension.getRecipient().getIdentity();
    }

    public String getCourseName() {
        if(ObjectUtils.anyNotNull(certificateExtension.getEvidence()) && StringUtils.isNotBlank(certificateExtension.getEvidence().getName())) {
            return certificateExtension.getEvidence().getName();
        } else {
            return "";
        }
    }

    public String getQrCodeImage() {
        try {
            URI uri = new URI(certificateExtension.getId());
            String path = uri.getPath();
            String idStr = path.substring(path.lastIndexOf('/') + 1);
            return StringUtils.substringBefore(idStr, ".") + ".png";
        } catch (URISyntaxException e) {
            return null;
        }
    }

    public String getIssuedDate() {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        String dateInFormat;
        try {
            Date parsedIssuedDate = simpleDateFormat.parse(certificateExtension.getIssuedOn());
            DateFormat format = new SimpleDateFormat("dd MMMM yyy", Locale.getDefault());
            format.format(parsedIssuedDate);
            dateInFormat = format.format(parsedIssuedDate);
            return dateInFormat;
        } catch (ParseException e) {
            return null;
        }
    }

    public String getSignatory0Image() {
        if (certificateExtension.getSignatory().length >= 1) {
            return certificateExtension.getSignatory()[0].getImage();
        } else {
            return "";
        }
    }

    public String getSignatory0Designation() {
        if (certificateExtension.getSignatory().length >= 1) {
            return certificateExtension.getSignatory()[0].getDesignation();
        } else {
            return "";
        }
    }

    public String getSignatory1Image() {
        if (certificateExtension.getSignatory().length >= 2) {
            return certificateExtension.getSignatory()[1].getImage();
        } else {
            return "";
        }
    }

    public String getSignatory1Designation() {
        if (certificateExtension.getSignatory().length >= 2) {
            return certificateExtension.getSignatory()[1].getDesignation();
        } else {
            return "";
        }
    }

    public String getCertificateName() {
        return certificateExtension.getBadge().getName();
    }

    public String getCertificateDescription() {
        return certificateExtension.getBadge().getDescription();
    }

    public String getExpiryDate() {
        return certificateExtension.getExpires();
    }

    public String getIssuerName() {
        return certificateExtension.getBadge().getIssuer().getName();
    }


    public Map<String, String> getCertMetaData() {
        Map<String, String> metaData = new HashMap<>();
        try {
            metaData.put(JsonKey.CERT_NAME, urlEncode(getCertificateName()));
            metaData.put(JsonKey.CERTIFICATE_DESCIPTION,urlEncode(getCertificateDescription()));
            metaData.put(JsonKey.COURSE_NAME, urlEncode(getCourseName()));
            metaData.put(JsonKey.ISSUE_DATE, urlEncode(getIssuedDate()));
            metaData.put(JsonKey.RECIPIENT_ID, urlEncode(getRecipientId()));
            metaData.put(JsonKey.RECIPIENT_NAME, urlEncode(getRecipientName()));
            metaData.put(JsonKey.SIGNATORY_0_IMAGE, urlEncode(getSignatory0Image()));
            metaData.put(JsonKey.SIGNATORY_0_DESIGNATION, urlEncode(getSignatory0Designation()));
            metaData.put(JsonKey.SIGNATORY_1_IMAGE, urlEncode(getSignatory1Image()));
            metaData.put(JsonKey.SIGNATORY_1_DESIGNATION, urlEncode(getSignatory1Designation()));
            metaData.put(JsonKey.EXPIRY_DATE, urlEncode(getExpiryDate()));
            metaData.put(JsonKey.ISSUER_NAME, urlEncode(getIssuerName()));
        } catch (UnsupportedEncodingException e) {
            logger.info("getCertMetaData: exception occurred while url encoding {}", e.getMessage());
        }
        return metaData;
    }

    private String urlEncode(String data) throws UnsupportedEncodingException {
        // URLEncoder.encode replace space with "+", but it should %20
        return StringUtils.isNotEmpty(data) ? URLEncoder.encode(data, "UTF-8").replace("+", "%20") : data;
    }

}
