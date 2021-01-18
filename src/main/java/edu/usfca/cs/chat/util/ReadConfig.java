package edu.usfca.cs.chat.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ReadConfig {
    private Properties prop = new Properties();
    private String propFileName = "config.properties";
//    private String propFileName = "./config.properties"; // For Jar File
    private InputStream inputStream;
    private String controllerHost;
    private int controllerPort;
    private int chunkSize;
    private String posixMountPoint;

    public String getMountPoint() {
        return mountPoint;
    }

    private int bloomFilterSize;
    private String sNDirPath;
    private int replicationFactor;
    private String mountPoint;

    public String getControllerHost() {
        return controllerHost;
    }

    public String getsNDirPath() {
        return sNDirPath;
    }

    public int getControllerPort() {
        return controllerPort;
    }

    public int getReplicationFactor() {
        return replicationFactor;
    }

    public int getBloomFilterSize() {
        return bloomFilterSize;
    }

    public ReadConfig() throws IOException {
        fillPropValues();
    }

    public int getChunkSize() {
        return chunkSize;
    }

    private void fillPropValues() throws IOException {
        try {
            prop = new Properties();
            inputStream = getClass().getClassLoader().getResourceAsStream(propFileName);

            if (inputStream != null) {
                prop.load(inputStream);
            } else {
                throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
            }
            controllerHost = prop.getProperty("controllerHost");
            sNDirPath = prop.getProperty("SNDirPath");
            controllerPort = Integer.parseInt(prop.getProperty("controllerPort"));
            bloomFilterSize = Integer.parseInt(prop.getProperty("bloomFilterSize"));
            replicationFactor = Integer.parseInt(prop.getProperty("replicationFactor"));
            chunkSize = Integer.parseInt(prop.getProperty("chunkSize"));
            mountPoint = prop.getProperty("mountPoint");
            posixMountPoint = prop.getProperty("posixMountPoint");

        } catch (Exception e) {
            System.out.println("Exception: " + e);
        } finally {
            inputStream.close();
        }
    }
}
