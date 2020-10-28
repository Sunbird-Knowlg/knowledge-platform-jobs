package org.sunbird.incredible.pojos;

import org.sunbird.incredible.pojos.ob.Evidence;

public class AssessedEvidence extends Evidence {
    /**
     * Subject of the evidence
     */
    private String subject;

    /**
     * Assessment conducted which elicited the evidence
     */
    private Assessment assessment;

    /**
     * HTTP URL identifier to a JSON-LD object or an embedded profile of the
     * individual(s) or organisation(s) who assessed the competency
     */
    private String assessedBy;

    /**
     * DateTime when the assessment was conducted
     */
    private String assessedOn;

    /**
     * Signature
     */
    private Signature signature;


    public AssessedEvidence(String ctx) {
        String[] type = new String[]{"Evidence", "Extension", "extensions:AssessedEvidence"};
        setType(type);
        setContext(ctx);
    }

    public String getSubject() {
        return subject;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }

    public Assessment getAssessment() {
        return assessment;
    }

    public void setAssessment(Assessment assessment) {
        this.assessment = assessment;
    }

    public String getAssessedBy() {
        return assessedBy;
    }

    public void setAssessedBy(String assessedBy) {
        this.assessedBy = assessedBy;
    }

    public String getAssessedOn() {
        return assessedOn;
    }

    public void setAssessedOn(String assessedOn) {
        this.assessedOn = assessedOn;
    }

    public Signature getSignature() {
        return signature;
    }

    public void setSignature(Signature signature) {
        this.signature = signature;
    }
}
