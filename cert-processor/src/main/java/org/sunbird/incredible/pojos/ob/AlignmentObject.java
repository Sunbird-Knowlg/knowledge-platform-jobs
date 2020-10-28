package org.sunbird.incredible.pojos.ob;

public class AlignmentObject extends OBBase {
    /**
     * Name of the alignment
     */
    private String targetName;

    /**
     * URL linking to the official description of the alignment target
     */
    private String targetURL;

    /**
     * Short description of the alignment target
     */
    private String targetDescription;

    /**
     * Name of the framework the alignment target
     */
    private String targetFramework;

    /**
     * If applicable, a locally unique string identifier that identifies the
     * alignment target within its framework and/or targetUrl
     */
    private String targetCode;

    public String getTargetName() {
        return targetName;
    }

    public void setTargetName(String targetName) {
        this.targetName = targetName;
    }

    public String getTargetURL() {
        return targetURL;
    }

    public void setTargetURL(String targetURL) {
        this.targetURL = targetURL;
    }

    public String getTargetDescription() {
        return targetDescription;
    }

    public void setTargetDescription(String targetDescription) {
        this.targetDescription = targetDescription;
    }

    public String getTargetFramework() {
        return targetFramework;
    }

    public void setTargetFramework(String targetFramework) {
        this.targetFramework = targetFramework;
    }

    public String getTargetCode() {
        return targetCode;
    }

    public void setTargetCode(String targetCode) {
        this.targetCode = targetCode;
    }
}
