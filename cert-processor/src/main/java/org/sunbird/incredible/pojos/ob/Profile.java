package org.sunbird.incredible.pojos.ob;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@JsonSerialize
public class Profile extends OBBase {
    /**
     * IRI to the awarding body, assessor or training body.
     */
    private String id;

    private String[] type;

    /**
     * Name of the awarding body, assessor or training body.
     */
    private String name;

    private String email;

    /**
     * URL of the awarding body, assessor or training body
     */
    private String url;

    /**
     * Short description
     */
    private String description;

    /**
     * URLs to type CryptographicKeys
     */
    private String[] publicKey;

    /**
     * List of HTTP URLs of the signed badges that are revoked
     */
    private String[] revocationList;

    // Part of OpenBadges, not mentioned in inCredible
    /**
     * A phone number
     */
    private String telephone;

    private VerificationObject verification;

    public Profile(String ctx) {
        setContext(ctx);
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String[] getType() {
        return type;
    }

    public void setType(String[] type) {
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String[] getPublicKey() {
        return publicKey;
    }

    public void setPublicKey(String[] publicKey) {
        this.publicKey = publicKey;
    }

    public String[] getRevocationList() {
        return revocationList;
    }

    public void setRevocationList(String[] revocationList) {
        this.revocationList = revocationList;
    }

    public String getTelephone() {
        return telephone;
    }

    public void setTelephone(String telephone) {
        this.telephone = telephone;
    }

    public VerificationObject getVerification() {
        return verification;
    }

    public void setVerification(VerificationObject verification) {
        this.verification = verification;
    }
}
