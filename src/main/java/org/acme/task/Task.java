package org.acme.task;

import io.agroal.api.AgroalDataSource;
import org.acme.CSVExpoter;
import org.acme.data.Files;
import org.acme.global.Globals;

import javax.inject.Inject;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public class Task implements Runnable {
    AgroalDataSource dataSource;

    @Inject
    Globals globals;

    private static Logger LOGGER = Logger.getLogger(Task.class.getName());

    private int rowStart = 0;
    private int rowEnd = 5000;
    private String beginDate;
    private String endDate;
    private String fileName;
    private String csvFileName;

    public Task(AgroalDataSource dataSource, Files files) {
        this.dataSource = dataSource;
        this.fileName = files.getFileName();
        this.beginDate = files.getBeginDate();
        this.endDate = files.getEndDate();
        this.csvFileName = files.getCsvFileName(fileName, beginDate, endDate);
    }

    @Override
    public synchronized void run() {
        try {
            CSVExpoter csvExpoter = new CSVExpoter();
            int rowcnt = 0;
            List<Map<String, Object>> listData = csvExpoter.getData(dataSource, rowStart, rowEnd, beginDate, endDate);
            BufferedWriter fileWriter = new BufferedWriter(new FileWriter(csvFileName));

            int colcnt = (listData.size() > 0) ? listData.get(0).size() : 0;
            csvExpoter.writeHeaderLine(listData.get(0), fileWriter);

            int csvCnt = csvExpoter.writeCSV(listData, fileWriter, colcnt);
            rowcnt = csvCnt;

            int totalCount = csvExpoter.getTotalCount();
            LOGGER.info("total count:" + totalCount + " start:" + rowStart + " size:" + rowEnd);

            int step = rowEnd - rowStart;
            while (rowcnt < totalCount) {
                rowStart = rowEnd + 1;
                listData = csvExpoter.getData(dataSource, rowStart, rowEnd += step, beginDate, endDate);
                csvCnt = csvExpoter.writeCSV(listData, fileWriter, colcnt);
                rowcnt += csvCnt;
                LOGGER.info("total count:" + totalCount + " start:" + rowStart + " size:" + rowEnd + " rowcnt:" + rowcnt);
                globals.setFileStatus(csvFileName ,"ingoing");
            }

            fileWriter.close();
            globals.setFileStatus(csvFileName, "done");
        } catch (Exception e) {
            e.printStackTrace();
        }
        LOGGER.info( "task complete " + rowStart + " - " + rowEnd);
    }
}
