package org.sunbird.incredible.processor.store;

import org.sunbird.cloud.storage.exception.StorageServiceException;

import java.io.File;

public abstract class CloudStore implements ICertStore {

    public CloudStore() {
    }

    @Override
    public String save(File file, String uploadPath) {
        return upload(file, uploadPath);
    }

    @Override
    public void get(String url, String fileName, String localPath) throws StorageServiceException {
        download(fileName, localPath);
    }

    @Override
    public void get(String fileName) throws StorageServiceException {
        String path = "conf/";
        download(fileName, path);
    }

    abstract public String upload(File file, String uploadPath);

    abstract public void download(String fileName, String localPath) throws StorageServiceException;

    abstract public void close();

}
