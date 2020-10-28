package org.sunbird.incredible.processor;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang.StringUtils;
import org.sunbird.incredible.builders.BadgeClassBuilder;
import org.sunbird.incredible.builders.CertificateExtensionBuilder;
import org.sunbird.incredible.builders.CompositeIdentityObjectBuilder;
import org.sunbird.incredible.builders.IssuerBuilder;
import org.sunbird.incredible.builders.SignatureBuilder;
import org.sunbird.incredible.builders.TrainingEvidenceBuilder;
import org.sunbird.incredible.processor.signature.SignatureHelper;
import org.sunbird.incredible.processor.signature.exceptions.SignatureException;
import org.sunbird.incredible.pojos.CertificateExtension;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.sunbird.incredible.pojos.ob.SignedVerification;
import org.sunbird.incredible.pojos.ob.exeptions.InvalidDateFormatException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CertificateFactory {

    private String uuid;

    private static Logger logger = LoggerFactory.getLogger(CertificateFactory.class);


    private ObjectMapper mapper = new ObjectMapper();

    public CertificateExtension createCertificate(CertModel certModel, Map<String, String> properties)
            throws InvalidDateFormatException, SignatureException.UnreachableException, IOException, SignatureException.CreationException {

        String basePath = getDomainUrl(properties);
        uuid = basePath + "/" + UUID.randomUUID().toString();
        CertificateExtensionBuilder certificateExtensionBuilder = new CertificateExtensionBuilder(properties.get(JsonKey.CONTEXT));
        CompositeIdentityObjectBuilder compositeIdentityObjectBuilder = new CompositeIdentityObjectBuilder(properties.get(JsonKey.CONTEXT));
        BadgeClassBuilder badgeClassBuilder = new BadgeClassBuilder(properties.get(JsonKey.CONTEXT));
        IssuerBuilder issuerBuilder = new IssuerBuilder(properties.get(JsonKey.CONTEXT));
        SignedVerification signedVerification = new SignedVerification();
        SignatureBuilder signatureBuilder = new SignatureBuilder();
        TrainingEvidenceBuilder trainingEvidenceBuilder = new TrainingEvidenceBuilder(properties.get(JsonKey.CONTEXT));


        /**
         *  recipient object
         *  **/
        compositeIdentityObjectBuilder.setName(certModel.getRecipientName()).setId(certModel.getIdentifier())
                .setHashed(false).
                setType(new String[]{JsonKey.ID});


        issuerBuilder.setId(properties.get(JsonKey.ISSUER_URL)).setName(certModel.getIssuer().getName())
                .setUrl(certModel.getIssuer().getUrl()).setPublicKey(certModel.getIssuer().getPublicKey());

        /**
         * badge class object
         * **/
        badgeClassBuilder.setDescription(certModel.getCertificateDescription())
                .setId(properties.get(JsonKey.BADGE_URL)).setCriteria(certModel.getCriteria())
                .setImage(certModel.getCertificateLogo()).
                setIssuer(issuerBuilder.build());
        //TODO for now badge name is set as course name, it should be certificate name
        //after certificate QR validation the portal uses badge name to display the course name,but course name is in the training evidence(introduced in release-1.5.0)
        //in evidence we are setting training name as courseName
        if(StringUtils.isNotEmpty(certModel.getCourseName())){
            badgeClassBuilder.setName(certModel.getCourseName());
        } else {
            badgeClassBuilder.setName(certModel.getCertificateName());
        }

        /**
         * Training evidence
         */
        if (StringUtils.isNotBlank(certModel.getCourseName())) {
            trainingEvidenceBuilder.setId(properties.get(JsonKey.EVIDENCE_URL)).setName(certModel.getCourseName());
            certificateExtensionBuilder.setEvidence(trainingEvidenceBuilder.build());
        }

        /**
         *
         * Certificate extension object
         */
        certificateExtensionBuilder.setId(uuid).setRecipient(compositeIdentityObjectBuilder.build())
                .setBadge(badgeClassBuilder.build())
                .setIssuedOn(certModel.getIssuedDate()).setExpires(certModel.getExpiry())
                .setValidFrom(certModel.getValidFrom()).setVerification(signedVerification).setSignatory(certModel.getSignatoryList());

        if (StringUtils.isEmpty(properties.get(JsonKey.KEY_ID))) {
            signedVerification.setType(new String[]{JsonKey.HOSTED});
            logger.info("CertificateExtension:createCertificate: if keyID is empty then verification type is HOSTED");
        } else {
            signedVerification.setCreator(properties.get(JsonKey.PUBLIC_KEY_URL));
            logger.info("CertificateExtension:createCertificate: if keyID is not empty then verification type is SignedBadge");

            /** certificate  signature value **/
            String signatureValue = getSignatureValue(certificateExtensionBuilder.build(), properties.get(JsonKey.ENC_SERVICE_URL), properties.get(JsonKey.KEY_ID));

            /**
             * to assign signature value
             */
            signatureBuilder.setCreated(Instant.now().toString()).setCreator(properties.get(JsonKey.SIGN_CREATOR))
                    .setSignatureValue(signatureValue);

            certificateExtensionBuilder.setSignature(signatureBuilder.build());
        }

        logger.info("CertificateFactory:createCertificate:certificate extension => {}", certificateExtensionBuilder.build());
        return certificateExtensionBuilder.build();
    }


    /**
     * to verifySignature signature value
     *
     * @param certificate
     * @param signatureValue
     * @param encServiceUrl
     * @return
     */
    public boolean verifySignature(JsonNode certificate, String signatureValue, String encServiceUrl, String creator) throws SignatureException.UnreachableException, SignatureException.VerificationException {
        boolean isValid = false;
        SignatureHelper signatureHelper = new SignatureHelper(encServiceUrl);
            Map<String, Object> signReq = new HashMap<>();
            signReq.put(JsonKey.CLAIM, certificate);
            signReq.put(JsonKey.SIGNATURE_VALUE, signatureValue);
            signReq.put(JsonKey.KEY_ID, getKeyId(creator));
            JsonNode jsonNode = mapper.valueToTree(signReq);
            isValid = signatureHelper.verifySignature(jsonNode);
        return isValid;
    }

    /**
     * to get signature value of certificate
     *
     * @param certificateExtension
     * @param encServiceUrl
     * @return
     */
    private String getSignatureValue(CertificateExtension certificateExtension, String encServiceUrl, String keyID) throws IOException, SignatureException.UnreachableException, SignatureException.CreationException {
        SignatureHelper signatureHelper = new SignatureHelper(encServiceUrl);
        Map<String, Object> signMap;
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        String request = mapper.writeValueAsString(certificateExtension);
        JsonNode jsonNode = mapper.readTree(request);
        logger.info("CertificateFactory:getSignatureValue:Json node of certificate".concat(jsonNode.toString()));
        signMap = signatureHelper.generateSignature(jsonNode, keyID);
        return (String) signMap.get(JsonKey.SIGNATURE_VALUE);

    }

    /**
     * to get the KeyId from the sign_creator url , key id used for verifying signature
     *
     * @param creator
     * @return
     */
    private int getKeyId(String creator) {
        String idStr = null;
        try {
            URI uri = new URI(creator);
            String path = uri.getPath();
            idStr = path.substring(path.lastIndexOf('/') + 1);
            idStr = idStr.split("_")[0];
            logger.info("CertificateFactory:getKeyId " + idStr);
        } catch (URISyntaxException e) {
            logger.debug("Exception while getting key id from the sign-creator url : {}", e.getMessage());
        }
        return Integer.parseInt(idStr);
    }


    /**
     * appends slug , org id, batch id to the domain url
     *
     * @param properties
     * @return
     */
    private String getDomainUrl(Map<String, String> properties) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(properties.get(JsonKey.BASE_PATH));
        if (StringUtils.isNotEmpty(properties.get(JsonKey.TAG))) {
            stringBuilder.append("/" + properties.get(JsonKey.TAG));
        }
        return stringBuilder.toString();
    }

}


