package org.sunbird.incredible.pojos;

public class Signature {
    private String type = "LinkedDataSignature2015";

    /**
     * Who created the signature?
     */
    private String creator;

    /**
     * Date timestamp of when the signature was created
     */
    private String created;

    /**
     * The signature hash of the certificate
     */
    private String signatureValue;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getCreator() {
        return creator;
    }

    public void setCreator(String creator) {
        this.creator = creator;
    }

    public String getCreated() {
        return created;
    }

    public void setCreated(String created) {
        this.created = created;
    }

    public String getSignatureValue() {
        return signatureValue;
    }

    public void setSignatureValue(String signatureValue) {
        this.signatureValue = signatureValue;
    }
}
