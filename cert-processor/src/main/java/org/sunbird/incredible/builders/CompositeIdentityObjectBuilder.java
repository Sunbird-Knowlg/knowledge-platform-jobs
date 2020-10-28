package org.sunbird.incredible.builders;

import org.sunbird.incredible.pojos.CompositeIdentityObject;
import org.sunbird.incredible.pojos.Gender;

public class CompositeIdentityObjectBuilder {

    private CompositeIdentityObject compositeIdentityObject = new CompositeIdentityObject();

    public CompositeIdentityObjectBuilder() {
    }

    public CompositeIdentityObjectBuilder(String context) {
        compositeIdentityObject.setContext(context);
    }

    public CompositeIdentityObjectBuilder setName(String name) {
        compositeIdentityObject.setName(name);
        return this;
    }

    public CompositeIdentityObjectBuilder setdob(String dob) {
        compositeIdentityObject.setDob(dob);
        return this;
    }

    public CompositeIdentityObjectBuilder setGender(Gender gender) {
        compositeIdentityObject.setGender(gender);
        return this;
    }

    public CompositeIdentityObjectBuilder setUrn(String urn) {
        compositeIdentityObject.setUrn(urn);
        return this;
    }

    public CompositeIdentityObjectBuilder setTag(String tag) {
        compositeIdentityObject.setTag(tag);
        return this;
    }

    public CompositeIdentityObjectBuilder setPhoto(String photo) {
        compositeIdentityObject.setPhoto(photo);
        return this;
    }

    public CompositeIdentityObjectBuilder setUrl(String url) {
        compositeIdentityObject.setUrl(url);
        return this;
    }

    public CompositeIdentityObjectBuilder setId(String id) {
        compositeIdentityObject.setIdentity(id);
        return this;
    }

    public CompositeIdentityObjectBuilder setType(String[] type) {
        compositeIdentityObject.setType(type);
        return this;
    }

    public CompositeIdentityObjectBuilder setHashed(Boolean hashed) {
        compositeIdentityObject.setHashed(hashed);
        return this;
    }

    public CompositeIdentityObject build() {
        return this.compositeIdentityObject;
    }
}
