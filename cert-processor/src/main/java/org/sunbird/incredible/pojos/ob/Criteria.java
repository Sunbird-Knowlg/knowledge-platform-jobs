package org.sunbird.incredible.pojos.ob;

public class Criteria extends OBBase {
    /**
     * Type of the criteria, defaults to "Criteria"
     */
    private String[] type = new String[]{"Criteria"};

    /**
     * IRI
     */
    private String id;

    /**
     * A narrative of what is needed to earn the badge
     */
    private String narrative;

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

    public String getNarrative() {
        return narrative;
    }

    public void setNarrative(String narrative) {
        this.narrative = narrative;
    }
}
