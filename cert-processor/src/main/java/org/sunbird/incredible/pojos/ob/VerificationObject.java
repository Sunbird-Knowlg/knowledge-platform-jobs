package org.sunbird.incredible.pojos.ob;

import java.util.List;

public class VerificationObject extends OBBase {
    /**
     * The type of verification method. Supported values for single assertion verification are HostedBadge and
     * SignedBadge (aliases in context are available: hosted and signed)
     */
    private String[] type;

    /**
     * The @id of the property to be used for verification that an Assertion is within the allowed scope. Only id is
     * supported. Verifiers will consider id the default value if verificationProperty is omitted or if an issuer
     * Profile has no explicit verification instructions, so it may be safely omitted.
     */
    private String verificationProperty;

    /**
     * The URI fragment that the verification property must start with. Valid Assertions must have an id within this
     * scope. Multiple values allowed, and Assertions will be considered valid if their id starts with one of these values.
     */
    private String startsWith;

    /**
     * The host registered name subcomponent of an allowed origin. Any given id URI will be considered valid.
     * Refer to https://tools.ietf.org/html/rfc3986#section-3.2.2
     * Example: ["example.org", "another.example.org"]
     */
    protected List<String> allowedOrigins;

    public VerificationObject() {}

    public String[] getType() {
        return type;
    }

    public void setType(String[] type) {
        this.type = type;
    }

    public String getVerificationProperty() {
        return verificationProperty;
    }

    public void setVerificationProperty(String verificationProperty) {
        this.verificationProperty = verificationProperty;
    }

    public String getStartsWith() {
        return startsWith;
    }

    public void setStartsWith(String startsWith) {
        this.startsWith = startsWith;
    }

    public List<String> getAllowedOrigins() {
        return allowedOrigins;
    }

    public void setAllowedOrigins(List<String> allowedOrigins) {
        this.allowedOrigins = allowedOrigins;
    }
}
