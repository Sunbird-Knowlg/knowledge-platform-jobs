package org.sunbird.incredible.processor.store;

import org.apache.commons.io.FileUtils;
import org.sunbird.incredible.processor.JsonKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.HttpMethod;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;

public class LocalStore implements ICertStore {

    private Logger logger = LoggerFactory.getLogger(LocalStore.class);

    private String domainUrl;

    public LocalStore(String domainUrl) {
        this.domainUrl = domainUrl;
    }

    @Override
    public String save(File file, String path) throws IOException {
        FileUtils.copyFileToDirectory(file, new File(path));
        return domainUrl + "/" + JsonKey.ASSETS + "/" + file.getName();
    }

    @Override
    public String getPublicLink(File file, String uploadPath) throws IOException {
        return save(file,uploadPath);
    }

    @Override
    public void get(String url, String fileName, String localPath) throws IOException {
        HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
        connection.setRequestMethod(HttpMethod.GET);
        InputStream inputStream = connection.getInputStream();
        FileOutputStream out = new FileOutputStream(localPath + fileName);
        copy(inputStream, out, 1024);
        out.close();
        inputStream.close();
        logger.info(fileName + " downloaded successfully");
    }

    @Override
    public void get(String fileName) {
    }

    @Override
    public void init() {

    }

    private void copy(InputStream input, OutputStream output, int bufferSize) throws IOException {
        byte[] buf = new byte[bufferSize];
        int read = input.read(buf);
        while (read >= 0) {
            output.write(buf, 0, read);
            read = input.read(buf);
        }
        output.flush();
    }

    @Override
    public void close(){
        
    }

}
