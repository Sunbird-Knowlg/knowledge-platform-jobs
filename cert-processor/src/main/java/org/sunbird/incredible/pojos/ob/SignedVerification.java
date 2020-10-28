package org.sunbird.incredible.pojos.ob;

public class SignedVerification extends VerificationObject {
    /**
     * The (HTTP) id of the key used to sign the Assertion. If not present, verifiers will check public key(s) declared
     * in the referenced issuer Profile. If a key is declared here, it must be authorized in the issuer Profile as well.
     * creator is expected to be the dereferencable URI of a document that describes a CryptographicKey
     */
    private String creator;

    public SignedVerification() {
        String[] type = new String[]{"SignedBadge"};
        setType(type);
    }

    public String getCreator() {
        return creator;
    }

    public void setCreator(String creator) {
        this.creator = creator;
    }
}
