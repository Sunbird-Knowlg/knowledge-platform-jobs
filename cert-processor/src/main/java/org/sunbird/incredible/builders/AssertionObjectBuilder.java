package org.sunbird.incredible.builders;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.sunbird.incredible.pojos.ob.Assertion;
import org.sunbird.incredible.pojos.ob.exeptions.InvalidDateFormatException;


public class AssertionObjectBuilder implements IBuilder<Assertion> {

    public Assertion assertion = new Assertion();


    private static ObjectMapper mapper = new ObjectMapper();


    public AssertionObjectBuilder setContext(String context) {
        assertion.setContext(context);
        return this;
    }


    public AssertionObjectBuilder setId(String id) {
        assertion.setId(id);
        return this;
    }

    public AssertionObjectBuilder setIssuedOn(String issuedOn) throws InvalidDateFormatException {
        assertion.setIssuedOn(issuedOn);
        return this;
    }

    public AssertionObjectBuilder setRecipient() {
        return this;
    }

    @Override
    public Assertion build() {
        return this.assertion;
    }

    @Override
    public String toString() {
        String stringRep = null;
        try {
            stringRep = mapper.writeValueAsString(this);
        } catch (JsonProcessingException jpe) {
        }
        return stringRep;
    }
}
