package com.ukchukx.rocksdbexample.repository;

import lombok.extern.slf4j.Slf4j;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.springframework.stereotype.Repository;
import org.springframework.util.SerializationUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Optional;

/**
 * @author HuberyYan
 */
@Slf4j
//@Repository
public class RocksDBRepositoryForByte {
    private String BASE_DIR = null;
    private String FILE_NAME = null;

    File baseDir;
    RocksDB db;

    public RocksDBRepositoryForByte(String baseDirStr, String fileNameStr){
        RocksDB.loadLibrary();
        final Options options = new Options();
        options.setCreateIfMissing(true);
//        if (BASE_DIR == null || FILE_NAME == null){
//            Properties pro = null;
//            try {
//                pro = PropertiesLoaderUtils.loadAllProperties("vod.properties");
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//            BASE_DIR = pro.getProperty("rocks_db_base_dir");
//            FILE_NAME = pro.getProperty("rocks_db_file_name");
//        }
        if (BASE_DIR == null || FILE_NAME == null){
            this.BASE_DIR = baseDirStr;
            this.FILE_NAME = fileNameStr;
        }
        baseDir = new File(BASE_DIR, FILE_NAME);
        try {
            Files.createDirectories(baseDir.getParentFile().toPath());
            Files.createDirectories(baseDir.getAbsoluteFile().toPath());
            db = RocksDB.open(options, baseDir.getAbsolutePath());

            log.info("RocksDB initialized");
        } catch (IOException | RocksDBException e) {
            log.error("Error initializng RocksDB. Exception: '{}', message: '{}'", e.getCause(), e.getMessage(), e);
        }

    }

    /**
     * // execute after the application starts.
     */
//    @PostConstruct
//    void initialize() {
//        RocksDB.loadLibrary();
//        final Options options = new Options();
//        options.setCreateIfMissing(true);
//        try {
//            Properties pro = PropertiesLoaderUtils.loadAllProperties("vod.properties");
//            BASE_DIR = pro.getProperty("rocks_db_base_dir");
//            FILE_NAME = pro.getProperty("rocks_db_file_name");
//            baseDir = new File("/tmp/rocks", FILE_NAME);
//    baseDir = new File("E:/SVN/new/rocks-db/tmp/rocks", FILE_NAME);
//            baseDir = new File(BASE_DIR, FILE_NAME);
//        }catch (Exception e){
//            e.printStackTrace();
//        }
//        try {
//            Files.createDirectories(baseDir.getParentFile().toPath());
//            Files.createDirectories(baseDir.getAbsoluteFile().toPath());
//            db = RocksDB.open(options, baseDir.getAbsolutePath());
//
//            log.info("RocksDB initialized");
//        } catch (IOException | RocksDBException e) {
//            log.error("Error initializng RocksDB. Exception: '{}', message: '{}'", e.getCause(), e.getMessage(), e);
//        }
//    }

//    @Override
    public synchronized boolean save(String key, Object value) {
        log.info("saving value '{}' with key '{}'", value, key);

        try {
            db.put(key.getBytes(), SerializationUtils.serialize(value));
        } catch (RocksDBException e) {
            log.error("Error saving entry. Cause: '{}', message: '{}'", e.getCause(), e.getMessage());

            return false;
        }

        return true;
    }

//    @Override
    public boolean saveByte(String key, byte[] value) {
        try {
            db.put(key.getBytes(), value);
        } catch (RocksDBException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

//    @Override
    public synchronized Optional<Object> find(String key) {
        return Optional.empty();
    }

//    @Override
    public byte[] findByte(String key) {
        byte[] bytes = new byte[0];
        try {
            bytes = db.get(key.getBytes());
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
        return bytes;
    }

//    @Override
    public synchronized boolean delete(String key) {
        log.info("deleting key '{}'", key);

        try {
            db.delete(key.getBytes());
        } catch (RocksDBException e) {
            log.error("Error deleting entry, cause: '{}', message: '{}'", e.getCause(), e.getMessage());

            return false;
        }

        return true;
    }
}
