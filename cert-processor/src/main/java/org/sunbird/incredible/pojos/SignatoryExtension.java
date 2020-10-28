package org.sunbird.incredible.pojos;

import org.sunbird.incredible.pojos.ob.CryptographicKey;
import org.sunbird.incredible.pojos.ob.IdentityObject;

public class SignatoryExtension extends IdentityObject {
    /**
     * Designation or capacity of the signatory
     */
    private String designation;

    /**
     * HTTP URL or a data URI for an image associated with the signatory
     */
    private String image;

    private CryptographicKey publicKey;

    private String name;

    public SignatoryExtension(String ctx) {
        String[] type = new String[]{"Extension", "extensions:SignatoryExtension"};
        setType(type);
        setContext(ctx);
    }
    public String getDesignation() {
        return designation;
    }

    public void setDesignation(String designation) {
        this.designation = designation;
    }

    public String getImage() {
        return image;
    }

    public void setImage(String image) {
        this.image = image;
    }

    public CryptographicKey getPublicKey() {
        return publicKey;
    }

    public void setPublicKey(CryptographicKey publicKey) {
        this.publicKey = publicKey;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
