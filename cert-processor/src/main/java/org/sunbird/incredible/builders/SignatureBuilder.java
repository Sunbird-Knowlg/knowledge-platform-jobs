package org.sunbird.incredible.builders;

import org.sunbird.incredible.pojos.Signature;

public class SignatureBuilder implements IBuilder<Signature> {

    Signature signature =  new Signature();


    public SignatureBuilder setType(String type) {
        signature.setType(type);
        return this;
    }


    public SignatureBuilder setCreator(String creator) {
        signature.setCreator(creator);
        return this;
    }


    public SignatureBuilder setCreated(String created) {
        signature.setCreated(created);
        return this;
    }



    public SignatureBuilder setSignatureValue(String signatureValue) {
        signature.setSignatureValue(signatureValue);
        return this;
    }
    @Override
    public Signature build() {
        return this.signature;
    }
}
