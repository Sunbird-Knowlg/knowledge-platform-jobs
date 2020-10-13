package org.sunbird.incredible.processor.store;

import org.sunbird.cloud.storage.exception.StorageServiceException;

import java.io.File;
import java.io.IOException;

public interface ICertStore {

    String save(File file, String uploadPath) throws IOException;

    String getPublicLink(File file, String uploadPath) throws IOException;

    void get(String url, String fileName, String localPath) throws IOException, StorageServiceException;

    void get(String fileName) throws StorageServiceException;

    void init();

    void close();


}
