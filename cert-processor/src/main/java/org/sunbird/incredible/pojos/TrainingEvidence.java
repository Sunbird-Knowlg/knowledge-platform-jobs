package org.sunbird.incredible.pojos;

import org.sunbird.incredible.pojos.ob.Evidence;

public class TrainingEvidence extends Evidence {
    private String subject;

    /**
     * HTTP URL identifier to a JSON-LD object or an embedded profile of the person or organisation entity(s) which provided training to the recipient of the credential
     */
    private String trainedBy;

    private Duration duration;

    /**
     * Denotes a session or a batch of the course that the recipient completed
     */
    private String session;

    public TrainingEvidence(String ctx) {
        String[] type = new String[]{"Evidence", "Extension", "extensions:TrainingEvidence"};
        setType(type);
        setContext(ctx);
    }

    public String getSubject() {
        return subject;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }

    public String getTrainedBy() {
        return trainedBy;
    }

    public void setTrainedBy(String trainedBy) {
        this.trainedBy = trainedBy;
    }

    public Duration getDuration() {
        return duration;
    }

    public void setDuration(Duration duration) {
        this.duration = duration;
    }

    public String getSession() {
        return session;
    }

    public void setSession(String session) {
        this.session = session;
    }
}
