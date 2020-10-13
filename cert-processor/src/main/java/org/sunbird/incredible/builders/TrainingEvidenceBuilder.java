package org.sunbird.incredible.builders;

import org.sunbird.incredible.pojos.Duration;
import org.sunbird.incredible.pojos.TrainingEvidence;

public class TrainingEvidenceBuilder implements IBuilder<TrainingEvidence> {

    private TrainingEvidence trainingEvidence ;

    public TrainingEvidenceBuilder(String context) {
        trainingEvidence = new TrainingEvidence(context);
    }

    public TrainingEvidenceBuilder setId(String id) {
        trainingEvidence.setId(id);
        return this;
    }

    public TrainingEvidenceBuilder setNarrative(String narrative) {
        trainingEvidence.setNarrative(narrative);
        return this;
    }

    public TrainingEvidenceBuilder setName(String name) {
        trainingEvidence.setName(name);
        return this;
    }

    public TrainingEvidenceBuilder setDescription(String description) {
        trainingEvidence.setDescription(description);
        return this;
    }

    public TrainingEvidenceBuilder setAudience(String audience) {
        trainingEvidence.setAudience(audience);
        return this;
    }

    public TrainingEvidenceBuilder setGenre(String genre) {
        trainingEvidence.setGenre(genre);
        return this;
    }


    public TrainingEvidenceBuilder setSubject(String subject) {
        trainingEvidence.setSubject(subject);
        return this;
    }


    public TrainingEvidenceBuilder setTrainedBy(String trainedBy) {
        trainingEvidence.setTrainedBy(trainedBy);
        return this;
    }


    public TrainingEvidenceBuilder setDuration(Duration duration) {
        trainingEvidence.setDuration(duration);
        return this;
    }


    public TrainingEvidenceBuilder setSession(String session) {
        trainingEvidence.setSession(session);
        return this;
    }

    @Override
    public TrainingEvidence build() {
        return this.trainingEvidence;
    }
}