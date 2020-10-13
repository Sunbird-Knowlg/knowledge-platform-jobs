package org.sunbird.incredible.processor.store;


import org.sunbird.incredible.UrlManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.sunbird.cloud.storage.BaseStorageService;
import scala.Option;

import java.io.File;

public class CloudStorage {

    private Logger logger = LoggerFactory.getLogger(CloudStorage.class);

    private  BaseStorageService storageService;


    public CloudStorage(BaseStorageService storageService) {
        this.storageService = storageService;
    }

    public  String upload(String container, String path, File file, boolean isDirectory,int retryCount) {
        String objectKey = path + file.getName();

        String url = storageService.upload(container,
                file.getAbsolutePath(),
                objectKey,
                Option.apply(isDirectory),
                Option.apply(1),
                Option.apply(retryCount), Option.apply(1));
        //storageService.upload() method returns signed url
        return UrlManager.removeQueryParams(url); //removes query params (signed text part)
    }

    public  String uploadFile(String container, String path, File file, boolean isDirectory,int retryCount) {
        String objectKey = path + file.getName();
        String url = storageService.upload(container,
                file.getAbsolutePath(),
                objectKey,
                Option.apply(isDirectory),
                Option.apply(1),
                Option.apply(retryCount), Option.apply(1));
        return UrlManager.getSharableUrl(url,container);
         }


    public void downloadFile(String container, String fileName, String localPath, boolean isDirectory) {
        storageService.download(container, fileName, localPath, Option.apply(isDirectory));
        logger.info(fileName + " downloaded successfully");
    }

    public void closeConnection(){
        storageService.closeContext();
    }
}
