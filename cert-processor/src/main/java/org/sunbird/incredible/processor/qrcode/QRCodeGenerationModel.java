package org.sunbird.incredible.processor.qrcode;


public class QRCodeGenerationModel {

    private String data;
    private String errorCorrectionLevel = "H";
    private int pixelsPerBlock = 2;
    private int qrCodeMargin = 3;
    private String text;
    private String textFontName = "Verdana";
    private int textFontSize = 16;
    private double textCharacterSpacing = 0.2;
    private int imageBorderSize = 0;
    private String colorModel = "Grayscale";
    private String fileName;
    private String fileFormat = "png";
    private int qrCodeMarginBottom = 1;
    private int imageMargin = 1;

    public int getImageMargin() {
        return imageMargin;
    }

    public void setImageMargin(int imageMargin) {
        this.imageMargin = imageMargin;
    }

    public int getQrCodeMarginBottom() {
        return qrCodeMarginBottom;
    }

    public void setQrCodeMarginBottom(int qrCodeMarginBottom) {
        this.qrCodeMarginBottom = qrCodeMarginBottom;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }

    public String getErrorCorrectionLevel() {
        return errorCorrectionLevel;
    }

    public void setErrorCorrectionLevel(String errorCorrectionLevel) {
        this.errorCorrectionLevel = errorCorrectionLevel;
    }

    public int getPixelsPerBlock() {
        return pixelsPerBlock;
    }

    public void setPixelsPerBlock(int pixelsPerBlock) {
        this.pixelsPerBlock = pixelsPerBlock;
    }

    public int getQrCodeMargin() {
        return qrCodeMargin;
    }

    public void setQrCodeMargin(int qrCodeMargin) {
        this.qrCodeMargin = qrCodeMargin;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public String getTextFontName() {
        return textFontName;
    }

    public void setTextFontName(String textFontName) {
        this.textFontName = textFontName;
    }

    public int getTextFontSize() {
        return textFontSize;
    }

    public void setTextFontSize(int textFontSize) {
        this.textFontSize = textFontSize;
    }

    public double getTextCharacterSpacing() {
        return textCharacterSpacing;
    }

    public void setTextCharacterSpacing(double textCharacterSpacing) {
        this.textCharacterSpacing = textCharacterSpacing;
    }

    public int getImageBorderSize() {
        return imageBorderSize;
    }

    public void setImageBorderSize(int imageBorderSize) {
        this.imageBorderSize = imageBorderSize;
    }

    public String getColorModel() {
        return colorModel;
    }

    public void setColorModel(String colorModel) {
        this.colorModel = colorModel;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public String getFileFormat() {
        return fileFormat;
    }

    public void setFileFormat(String fileFormat) {
        this.fileFormat = fileFormat;
    }
}
