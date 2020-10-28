package org.sunbird.incredible.pojos.ob;

public class CryptographicKey extends OBBase {
    private String[] type = new String[]{"CryptographicKey"};
    private String id;
    private String owner;
    private String publicKeyPem;

    public CryptographicKey(String ctx) {
        setContext(ctx);
    }

    public String[] getType() {
        return type;
    }

    public void setType(String[] type) {
        this.type = type;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public String getPublicKeyPem() {
        return publicKeyPem;
    }

    public void setPublicKeyPem(String publicKeyPem) {
        this.publicKeyPem = publicKeyPem;
    }
}
