package org.sunbird.incredible.builders;

import org.sunbird.incredible.pojos.ob.AlignmentObject;
import org.sunbird.incredible.pojos.ob.BadgeClass;
import org.sunbird.incredible.pojos.ob.Criteria;
import org.sunbird.incredible.pojos.ob.Issuer;


public class BadgeClassBuilder implements IBuilder<BadgeClass> {


    private BadgeClass badgeClass;

    public BadgeClassBuilder(String context) {
        badgeClass = new BadgeClass(context);
    }

    public BadgeClassBuilder setId(String id) {
        badgeClass.setId(id);
        return this;

    }


    public BadgeClassBuilder setType(String[] type) {
        badgeClass.setType(type);
        return this;
    }


    public BadgeClassBuilder setName(String name) {
        badgeClass.setName(name);
        return this;
    }


    public BadgeClassBuilder setDescription(String description) {
        badgeClass.setDescription(description);
        return this;
    }


    public BadgeClassBuilder setImage(String image) {
        badgeClass.setImage(image);
        return this;
    }


    public BadgeClassBuilder setCriteria(Criteria criteria) {
        badgeClass.setCriteria(criteria);
        return this;
    }


    public BadgeClassBuilder setIssuer(Issuer issuer) {
        badgeClass.setIssuer(issuer);
        return this;
    }


    public BadgeClassBuilder setAlignment(AlignmentObject alignment) {
        badgeClass.setAlignment(alignment);
        return this;
    }


    @Override
    public BadgeClass build() {
        return this.badgeClass;
    }
}
