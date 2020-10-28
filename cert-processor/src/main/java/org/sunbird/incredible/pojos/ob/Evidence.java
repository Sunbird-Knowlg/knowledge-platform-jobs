package org.sunbird.incredible.pojos.ob;




public class Evidence extends OBBase {
    /**
     * identifies the evidence from the urn:uuid namespace or a HTTP URL of a
     * webpage which presents the evidence
     */
    private String id;

    private String[] type;

    /**
     * A narrative that describes the evidence and process of achievement that
     * led to an Assertion
     */
    private String narrative;

    /**
     * Descriptive title of the evidence
     */
    private String name;

    /**
     * Longer description of the evidence
     */
    private String description;

    /**
     * Describes the type of evidence, such as Certificate, Painting, Artefact,
     * Medal, Video, Image.
     */
    private String genre;

    /**
     * Description of the intended audience for a piece of evidence
     */
    private String audience;

    protected Evidence() {}

    public Evidence(String ctx) {
        setContext(ctx);
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String[] getType() {
        return type;
    }

    public void setType(String[] type) {
        this.type = type;
    }

    public String getNarrative() {
        return narrative;
    }

    public void setNarrative(String narrative) {
        this.narrative = narrative;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getGenre() {
        return genre;
    }

    public void setGenre(String genre) {
        this.genre = genre;
    }

    public String getAudience() {
        return audience;
    }

    public void setAudience(String audience) {
        this.audience = audience;
    }
}
