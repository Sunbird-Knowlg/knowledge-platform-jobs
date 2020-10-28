package org.sunbird.incredible.pojos;

public class RankAssessment extends Assessment {
    /**
     * Maximum score that must be achieved for qualification
     */
    private float maxValue;

    public RankAssessment() {
        String[] type = new String[]{"Extension", "extensions:RankAssessment"};
        setType(type);
    }

    public float getMaxValue() {
        return maxValue;
    }

    public void setMaxValue(float maxValue) {
        this.maxValue = maxValue;
    }
}
