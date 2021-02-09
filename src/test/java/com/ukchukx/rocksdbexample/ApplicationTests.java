package com.ukchukx.rocksdbexample;

import com.ukchukx.rocksdbexample.repository.RocksDBRepositoryForByte;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = Application.class)
public class ApplicationTests {

    String path = "E:\\SVN\\rocks-db\\tmp\\rocks";
    String fileName = "cache-rocks-db";
    String fileName2 = "spring-boot-db";

    @Test
    public void contextLoads() {
        RocksDBRepositoryForByte iterator = new RocksDBRepositoryForByte(path, fileName);
//        iterator.saveByte()
//        byte[] aByte = iterator.findByte("987858a1a8a6489aa5ea9f2f9087db3a");
//        System.out.println(aByte);
        iterator.iterator();
    }

    @Test
    public void test() {
        RocksDBRepositoryForByte iterator = new RocksDBRepositoryForByte(path, fileName);
        long totalCount = iterator.getTotalCount();
        System.out.println(totalCount);
    }

}
