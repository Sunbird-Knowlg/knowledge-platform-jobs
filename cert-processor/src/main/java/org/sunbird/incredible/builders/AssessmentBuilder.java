package org.sunbird.incredible.builders;

import org.sunbird.incredible.pojos.Assessment;

public class AssessmentBuilder implements IBuilder<Assessment> {


    Assessment assessment;

    public AssessmentBuilder(String context) {
        assessment = new Assessment(context);
    }

    public AssessmentBuilder setType(String[] type) {
        assessment.setType(type);
        return this;
    }


    public AssessmentBuilder setValue(float value) {
        assessment.setValue(value);
        return this;
    }

    @Override
    public Assessment build() {
        return this.assessment;
    }
}
