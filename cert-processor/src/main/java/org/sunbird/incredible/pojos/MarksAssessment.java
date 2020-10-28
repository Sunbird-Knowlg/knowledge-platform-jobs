package org.sunbird.incredible.pojos;

public class MarksAssessment extends Assessment {
    /**
     * Minimum score that must be achieved for qualification
     */
    private float minValue;

    /**
     * Maximum score that must be achieved for qualification
     */
    private float maxValue;

    /**
     * Actual score achieved
     */
    private float passValue;

    public MarksAssessment() {
        String[] type = new String[] {"Extension", "extensions:MarksAssessment"};
        setType(type);
    }

    public float getMinValue() {
        return minValue;
    }

    public void setMinValue(float minValue) {
        this.minValue = minValue;
    }

    public float getMaxValue() {
        return maxValue;
    }

    public void setMaxValue(float maxValue) {
        this.maxValue = maxValue;
    }

    public float getPassValue() {
        return passValue;
    }

    public void setPassValue(float passValue) {
        this.passValue = passValue;
    }
}
