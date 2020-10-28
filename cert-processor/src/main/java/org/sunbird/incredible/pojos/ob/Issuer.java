package org.sunbird.incredible.pojos.ob;

public class Issuer extends OBBase {

    private String id;

    private String[] type;


    private String context;

    /**
     * Name of the awarding body, assessor or training body.
     */
    private String name;

    private String email;

    /**
     * URL of the awarding body, assessor or training body
     * Mandatory member
     */
    private String url;

    /**
     * URLs to type CryptographicKeys
     */
    private String[] publicKey;


    public Issuer(String ctx) {
        setContext(ctx);
        setType(new String[]{"Issuer"});
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

    @Override
    public String getContext() {
        return context;
    }

    @Override
    public void setContext(String context) {
        this.context = context;
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

    public String[] getPublicKey() {
        return publicKey;
    }

    public void setPublicKey(String[] publicKey) {
        this.publicKey = publicKey;
    }
}
