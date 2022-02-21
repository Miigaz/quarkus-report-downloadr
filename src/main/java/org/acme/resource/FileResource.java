package org.acme.resource;

import io.agroal.api.AgroalDataSource;
import io.vertx.core.json.JsonObject;
import org.acme.CSVExpoter;
import org.acme.data.Files;
import org.acme.global.Globals;
import org.acme.task.Task;

import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

@Path("/f")
public class FileResource {
    private static Logger LOGGER = Logger.getLogger(FileResource.class.getName());

    @Inject
    AgroalDataSource dataSource;

    @Inject
    Globals globals;

    @POST
    @Path("/files")
    @Produces(MediaType.APPLICATION_JSON)
    public Response downloadFileRequest(Files files) {
        String csvFileName = files.getCsvFileName(files.getFileName(), files.getBeginDate(), files.getEndDate());

        Runnable r1 = new Task(dataSource, files);
        ExecutorService pool = Executors.newSingleThreadExecutor();
        pool.execute(r1);
        pool.shutdown();

        JsonObject jsob = new JsonObject();
        jsob.put("fileName", csvFileName);

        return Response.ok(jsob).build();
    }

    @POST
    @Path("/files/status/{filename}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getFileStatus(@PathParam("filename") String fileName) {
        LOGGER.info("fileStatus: " + globals.getFileStatus(fileName));

        JsonObject jsob = new JsonObject();
        jsob.put("status", Globals.getFileStatus(fileName));
        return Response.ok(jsob).build();
    }

    @POST
    @Path("/files/download/{filename}")
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    public Response downloadFile(@PathParam("filename") String fileName) {
        if(globals.getFileStatus(fileName) == "done") {
            File fileDownload = new File("D:/internship/quarkus-report-downloader/" + fileName);
            LOGGER.info("File: " + fileName);
            LOGGER.info("file exists: " + fileDownload.exists());

            Response.ResponseBuilder response = Response.ok(fileDownload);
            response.header("Content-Disposition", "attachment;filename=" + fileName);

            globals.setFileStatus(fileName, "dead");
            return response.build();
        }
        return Response.status(400).build();
    }
}