package org.sunbird.incredible.processor.store;


import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sunbird.cloud.storage.BaseStorageService;
import org.sunbird.cloud.storage.factory.StorageConfig;
import org.sunbird.cloud.storage.factory.StorageServiceFactory;

/**
 * used to upload or download files to the azure
 */
import java.io.File;

public class AzureStore extends CloudStore {

    private Logger logger = LoggerFactory.getLogger(AzureStore.class);

    private StoreConfig azureStoreConfig;

    private BaseStorageService storageService = null;

    private CloudStorage cloudStorage = null;

    private int retryCount = 0;

    public AzureStore(StoreConfig azureStoreConfig) {
        this.azureStoreConfig = azureStoreConfig;
        retryCount = Integer.parseInt(azureStoreConfig.getCloudRetryCount());
        init();
    }

    @Override
    public String upload(File file, String path) {
        String uploadPath = getPath(path);
        return cloudStorage.uploadFile(azureStoreConfig.getAzureStoreConfig().getContainerName(), uploadPath, file, false, retryCount);
    }

    private String getPath(String path) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(path);
        if (StringUtils.isNotBlank(azureStoreConfig.getAzureStoreConfig().getPath())) {
            stringBuilder.append(azureStoreConfig.getAzureStoreConfig().getPath() + "/");
        }
        return stringBuilder.toString();
    }

    @Override
    public void download(String fileName, String localPath) {
        cloudStorage.downloadFile(azureStoreConfig.getAzureStoreConfig().getContainerName(), fileName, localPath, false);
    }

    @Override
    public String getPublicLink(File file, String uploadPath) {
        String path = getPath(uploadPath);
        return cloudStorage.upload(azureStoreConfig.getAzureStoreConfig().getContainerName(), path, file, false, retryCount);
    }

    @Override
    public void init() {
        if (StringUtils.isNotBlank(azureStoreConfig.getType())) {
            String storageKey = azureStoreConfig.getAzureStoreConfig().getAccount();
            String storageSecret = azureStoreConfig.getAzureStoreConfig().getKey();
            StorageConfig storageConfig = new StorageConfig(azureStoreConfig.getType(), storageKey, storageSecret);
            logger.info("StorageParams:init:all storage params initialized for azure block");
            storageService = StorageServiceFactory.getStorageService(storageConfig);
            cloudStorage = new CloudStorage(storageService);
        } else {
            logger.error("StorageParams:init:provided cloud store type doesn't match supported storage devices:".concat(azureStoreConfig.getType()));
        }

    }

    @Override
    public void close(){
        cloudStorage.closeConnection();
    }
}

