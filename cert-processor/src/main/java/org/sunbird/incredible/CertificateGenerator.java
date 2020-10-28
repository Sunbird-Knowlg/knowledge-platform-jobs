package org.sunbird.incredible;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.zxing.NotFoundException;
import com.google.zxing.WriterException;
import org.apache.commons.lang.StringUtils;
import org.sunbird.incredible.processor.CertModel;
import org.sunbird.incredible.processor.CertificateFactory;
import org.sunbird.incredible.processor.JsonKey;
import org.sunbird.incredible.processor.qrcode.AccessCodeGenerator;
import org.sunbird.incredible.processor.qrcode.QRCodeGenerationModel;
import org.sunbird.incredible.processor.qrcode.utils.QRCodeImageGenerator;
import org.sunbird.incredible.processor.signature.exceptions.SignatureException;
import org.sunbird.incredible.pojos.CertificateExtension;
import org.sunbird.incredible.pojos.ob.exeptions.InvalidDateFormatException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.*;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

/**
 * Generates certificate json, qrcode , html and pdf
 */
public class CertificateGenerator {


    private static Logger logger = LoggerFactory.getLogger(CertificateGenerator.class);

    private Map<String, String> properties;

    private ObjectMapper objectMapper = new ObjectMapper();

    private CertificateFactory certificateFactory = new CertificateFactory();

    private CertificateExtension certificateExtension;

    private String directory;

    public CertificateGenerator(Map<String, String> properties, String directory) {
        this.properties = properties;
        this.directory = directory;
    }

    public CertificateGenerator(Map<String, String> properties) {
        this.properties = properties;
    }

    public CertificateExtension getCertificateExtension (CertModel certModel) throws SignatureException.UnreachableException,
            InvalidDateFormatException, SignatureException.CreationException, IOException {
        this.certificateExtension = certificateFactory.createCertificate(certModel, properties);
        return certificateExtension;
    }

    public String getUUID(CertificateExtension certificateExtension) {
        String idStr;
        try {
            URI uri = new URI(certificateExtension.getId());
            String path = uri.getPath();
            idStr = path.substring(path.lastIndexOf('/') + 1);
        } catch (URISyntaxException e) {
            return null;
        }
        return StringUtils.substringBefore(idStr, ".");
    }

    public String generateCertificateJson(CertificateExtension certificateExtension) throws IOException {
        checkDirectoryExists();
        File file = new File(directory + getUUID(certificateExtension) + ".json");
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        objectMapper.writeValue(file, certificateExtension);
        String jsonData = objectMapper.writeValueAsString(certificateExtension);
        logger.info("Json file has been generated for the certificate");
        return jsonData;
    }

    private void checkDirectoryExists() {
        File file = new File(directory);
        if (!file.exists()) {
            logger.info("File directory does not exist."+file.getName());
            file.mkdirs();
        }
    }

    public Map<String,Object> generateQrCode() throws WriterException,
            FontFormatException, NotFoundException, IOException {
        checkDirectoryExists();
        Map<String,Object> qrMap = new HashMap<>();
        AccessCodeGenerator accessCodeGenerator = new AccessCodeGenerator(Double.valueOf(properties.get(JsonKey.ACCESS_CODE_LENGTH)));
        String accessCode = accessCodeGenerator.generate();
        QRCodeGenerationModel qrCodeGenerationModel = new QRCodeGenerationModel();
        qrCodeGenerationModel.setText(accessCode);
        qrCodeGenerationModel.setFileName(directory + getUUID(certificateExtension));
        qrCodeGenerationModel.setData(properties.get(JsonKey.BASE_PATH).concat("/") + getUUID(certificateExtension));
        QRCodeImageGenerator qrCodeImageGenerator = new QRCodeImageGenerator();
        File qrCodeFile = qrCodeImageGenerator.createQRImages(qrCodeGenerationModel);

        qrMap.put(JsonKey.QR_CODE_FILE,qrCodeFile);
        qrMap.put(JsonKey.ACCESS_CODE,accessCode);
        logger.info("Qrcode {} is created for the certificate", qrCodeFile.getName());
        return qrMap;
    }

}