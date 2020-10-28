package org.sunbird.incredible.pojos;

import org.sunbird.incredible.pojos.ob.IdentityObject;

import java.util.List;

public class CompositeIdentityObject extends IdentityObject {
    private String[] type = new String[]{"Extension", "IdentityObject", "extensions:CompositeIdentityObject"};
    private List<CompositeIdentityObject> components;

    private String name;
    /**
     * A HTTP URL to a printable version of this certificate.
     * The URL points to a base64 encoded data, like data:application/pdf;base64
     * or data:image/jpeg;base64
     */
    private String photo;

    /**
     * Date of birth in YYYY-MM-DD format.
     */
    private String dob;

    /**
     * Gender
     */
    private Gender gender;

    /**
     * Tag URI, if URN is used
     */
    private String tag;

    /**
     * Uniform resource name
     * Required in the absence of URL
     */
    private String urn;

    /**
     * Uniform resource locator
     * Required in the absence of URN
     */
    private String url;

    public CompositeIdentityObject() {
        setType(type);
    }

    public List<CompositeIdentityObject> getComponents() {
        return components;
    }

    public void setComponents(List<CompositeIdentityObject> components) {
        this.components = components;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getPhoto() {
        return photo;
    }

    public void setPhoto(String photo) {
        this.photo = photo;
    }

    public String getDob() {
        return dob;
    }

    public void setDob(String dob) {
        this.dob = dob;
    }

    public Gender getGender() {
        return gender;
    }

    public void setGender(Gender gender) {
        this.gender = gender;
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public String getUrn() {
        return urn;
    }

    public void setUrn(String urn) {
        this.urn = urn;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }
}
