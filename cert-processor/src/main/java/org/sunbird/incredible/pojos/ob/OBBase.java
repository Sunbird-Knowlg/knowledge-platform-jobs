package org.sunbird.incredible.pojos.ob;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Common properties that are applicable to several classes of the OpenBadges
 */
public class OBBase {
    /**
     * The context.
     */
    @JsonProperty("@context")
    private String context;

    /**
     * Identifies a related version of the entity.
     */
    private String[] related;

    /**
     * The version identifier for the present edition of the entity.
     */
    private String version;

    /**
     * A claim made about this entity.
     */
    private Endorsement endorsement;

    private static final ObjectMapper mapper = new ObjectMapper();

    protected OBBase() {
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    }

    public String getContext() {
        return context;
    }

    public void setContext(String context) {
        this.context = context;
    }

    public String[] getRelated() {
        return related;
    }

    public void setRelated(String[] related) {
        this.related = related;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public Endorsement getEndorsement() {
        return endorsement;
    }

    public void setEndorsement(Endorsement endorsement) {
        this.endorsement = endorsement;
    }

    @Override
    public String toString() {
        String stringRep = null;
        try {
            stringRep = mapper.writeValueAsString(this);
        } catch (JsonProcessingException jpe) {
            jpe.printStackTrace();
        }
        return stringRep;
    }
}
