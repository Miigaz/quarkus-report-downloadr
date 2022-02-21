package org.acme.global;

import javax.enterprise.context.ApplicationScoped;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

@ApplicationScoped
public class Globals {
    public static Logger LOGGER = Logger.getLogger(Globals.class.getName());
    public static Map<String, String> fileStatus = new HashMap<>();

    public static void setFileStatus(String fileName, String status) {
        fileStatus.put(fileName, status);
        LOGGER.info("file status: " + fileStatus.get(fileName));
    }

    public static String getFileStatus(String fileName) {
//        LOGGER.info("Stat: " + fileStatus);

        return fileStatus.get(fileName);
    }

}
