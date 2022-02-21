package org.acme.data;

import javax.validation.constraints.NotBlank;

public class Files {
    @NotBlank()
    private String fileName;

    @NotBlank()
    private String beginDate;

    @NotBlank()
    private String endDate;

    public Files() {
    }

    public String getCsvFileName(String baseName, String beginDate, String endDate) {
        return baseName.concat(String.format("_%s~%s.csv", beginDate, endDate));
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public void setBeginDate(String beginDate) {
        this.beginDate = beginDate;
    }

    public void setEndDate(String endDate) {
        this.endDate = endDate;
    }

    public String getFileName() {
        return fileName;
    }

    public String getBeginDate() {
        return beginDate;
    }

    public String getEndDate() {
        return endDate;
    }

}
