package org.sunbird.incredible.processor.store;


import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.commons.lang.StringUtils;
import org.sunbird.incredible.processor.JsonKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

public class CertStoreFactory {


    private final Logger logger = LoggerFactory.getLogger(getClass());

    private Map<String, String> properties;


    public CertStoreFactory(Map<String, String> properties) {
        this.properties = properties;
    }

    /**
     * templateUrl could be local and/or relative or http URL
     * RELATIVE: If storageParams exist and url doesn't start with http, it is relative to container.
     * ABSOLUTE: a)If storageParams exist and url starts with http,then it is httpURL (private, container only)
     * b)if storageParams does not exits, then it is httpURL (public).
     * LOCAL- If storageParams doesn't exist, and  url is always relative and then template is in local
     *
     * @param templateUrl
     * @param storeConfig storage params
     * @return
     */
    public ICertStore getHtmlTemplateStore(String templateUrl, StoreConfig storeConfig) {
        ICertStore certStore = null;
        if (templateUrl.startsWith("http")) {
            if (StringUtils.isNotBlank(storeConfig.getContainerName()) &&
                    templateUrl.contains(storeConfig.getContainerName()) &&
                    storeConfig.isCloudStore()) {
                certStore = getCloudStore(storeConfig);
            } else {
                certStore = new LocalStore(properties.get(JsonKey.DOMAIN_URL));
            }
        } else if (storeConfig.isCloudStore()) {
            certStore = getCloudStore(storeConfig);
        }
        return certStore;
    }

    /**
     * used to know whether certificate files should be stored in local or cloud
     * Scenario 1)If storage params exits then it is cloud storage
     * Scenario 2)If preview is true (even If storage params exists it not cloud store),then it always local store
     * Scenario 3)If storage params doest not exits , then it is local store
     *
     * @param storeConfig
     * @param preview
     * @return
     */
    public ICertStore getCertStore(StoreConfig storeConfig, Boolean preview) {
        ICertStore store = null;
        try {
            String domainUrl = StringUtils.remove(properties.get(JsonKey.BASE_PATH), new URL(properties.get(JsonKey.BASE_PATH)).getPath());
            if (preview) {
                store = new LocalStore(domainUrl);
            } else if (storeConfig.isCloudStore()) {
                store = getCloudStore(storeConfig);
            } else {
                store = new LocalStore(domainUrl);
            }
        } catch (MalformedURLException e) {

        }
        return store;
    }

    /**
     * used to clean up files which start with uuid.*
     *
     * @param fileName
     * @param path
     */
    public void cleanUp(String fileName, String path) {
        Boolean isDeleted = false;
        try {
            if (StringUtils.isNotBlank(fileName)) {
                File directory = new File(path);
                Collection<File> files = FileUtils.listFiles(directory, new WildcardFileFilter(fileName + ".*"), null);
                Iterator iterator = files.iterator();
                while (iterator.hasNext()) {
                    File file = (File) iterator.next();
                    isDeleted = file.delete();
                }
                logger.info("files cleanUp completed: " + isDeleted);
            }
        } catch (Exception ex) {
            logger.error(ex.getMessage(), ex);
        }
    }

    /**
     * returns directory name to store all the certificate related files
     *
     * @param zipFileName
     * @return
     */
    public String getDirectoryName(String zipFileName) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("conf/");
        if (StringUtils.isNotEmpty(properties.get(JsonKey.TAG))) {
            stringBuilder.append(properties.get(JsonKey.TAG) + "_");
        }
        return stringBuilder.toString().concat(zipFileName.concat("/"));
    }

    /**
     * to know whether cloud store is azure or aws
     *
     * @param storeConfig
     * @return instance of azureStore or awsStore
     */
    public CloudStore getCloudStore(StoreConfig storeConfig) {
        CloudStore cloudStore = null;
        if (JsonKey.AZURE.equals(storeConfig.getType())) {
            cloudStore = new AzureStore(storeConfig);
        } else if (JsonKey.AWS.equals(storeConfig.getType())) {
            cloudStore = new AwsStore(storeConfig);
        }
        return cloudStore;
    }

    /**
     * set the path for file to store cloud or path to store in local
     *
     * @param storeConfig
     * @return
     */
    public String setCloudPath(StoreConfig storeConfig) {
        StringBuilder stringBuilder = new StringBuilder();
 if (storeConfig.isCloudStore()) {
            String tagId = properties.get(JsonKey.TAG);
            if (StringUtils.isNotEmpty(tagId)) {
                stringBuilder.append(tagId).append("/");
            }
        } else {
            stringBuilder.append("public/").toString();

        }
        return stringBuilder.toString();
    }


}


