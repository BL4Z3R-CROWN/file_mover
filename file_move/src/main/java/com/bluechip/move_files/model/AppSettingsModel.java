package com.bluechip.move_files.model;

public class AppSettingsModel {
    private int id;
    private String decomprWatchedPath;
    private String profilingDirectory;
    private String fileInfoTableName;
    private String identity;

    public int getId() {
        return this.id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getDecomprWatchedPath() {
        return this.decomprWatchedPath;
    }

    public void setDecomprWatchedPath(String decomprWatchedPath) {
        this.decomprWatchedPath = decomprWatchedPath;
    }

    public String getProfilingDirectory() {
        return this.profilingDirectory;
    }

    public void setProfilingDirectory(String profilingDirectory) {
        this.profilingDirectory = profilingDirectory;
    }

    public String getFileInfoTableName() {
        return this.fileInfoTableName;
    }

    public void setFileInfoTableName(String fileInfoTableName) {
        this.fileInfoTableName = fileInfoTableName;
    }

    public String getIdentity() {
        return this.identity;
    }

    public void setIdentity(String identity) {
        this.identity = identity;
    }
}

