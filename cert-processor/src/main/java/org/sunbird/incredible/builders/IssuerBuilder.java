package org.sunbird.incredible.builders;

import org.sunbird.incredible.pojos.ob.Issuer;

public class IssuerBuilder implements IBuilder<Issuer> {

    Issuer issuer;


    public IssuerBuilder(String context) {
        issuer = new Issuer(context);
    }

    public IssuerBuilder setId(String id) {
        issuer.setId(id);
        return this;
    }

    public IssuerBuilder setType(String[] type) {
        issuer.setType(type);
        return this;
    }

    public IssuerBuilder setName(String name) {
        issuer.setName(name);
        return this;
    }

    public IssuerBuilder setEmail(String email) {
        issuer.setEmail(email);
        return this;
    }

    public IssuerBuilder setUrl(String url) {
        issuer.setUrl(url);
        return this;
    }

    public IssuerBuilder setPublicKey(String[] publicKey) {
        issuer.setPublicKey(publicKey);
        return this;
    }

    @Override
    public Issuer build() {
        return this.issuer;
    }
}
