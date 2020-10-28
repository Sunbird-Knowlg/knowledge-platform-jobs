package org.sunbird.incredible.pojos.ob;

public class Endorsement extends OBBase {
    /**
     * Unique IRI for the Endorsement instance.
     * If using hosted verification, this should be the URI where the assertion of endorsement is accessible.
     * For signed Assertions, it is recommended to use a UUID in the urn:uuid namespace.
     */
    private String id;

    private String[] type = new String[]{"Endorsement"};

    /**
     * An entity, identified by an id and additional properties that the endorser would like to claim about that entity.
     */
    private String claim;

    /**
     * The profile of the Endorsement’s issuer.
     */
    private Profile issuer;

    /**
     * Timestamp of when the endorsement was published.
     */
    private String issuedOn;

    /**
     * Instructions for third parties to verify this assertion of endorsement.
     */
    private VerificationObject verification;

    public Endorsement(String ctx) {
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

    public String getClaim() {
        return claim;
    }

    public void setClaim(String claim) {
        this.claim = claim;
    }

    public Profile getIssuer() {
        return issuer;
    }

    public void setIssuer(Profile issuer) {
        this.issuer = issuer;
    }

    public String getIssuedOn() {
        return issuedOn;
    }

    public void setIssuedOn(String issuedOn) {
        this.issuedOn = issuedOn;
    }

    public VerificationObject getVerification() {
        return verification;
    }

    public void setVerification(VerificationObject verification) {
        this.verification = verification;
    }
}
