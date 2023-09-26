package cache;

import file.BlockId;
import file.FileManager;
import file.Page;
import log.LogManager;
import server.Database;

public class Test {
    public static void main(String[] args) {
        //testFile();
        testPin();
    }

    public static void testBufferWrite() {
        Database.Cache_SIZE=8;
        Database database=new Database("testDB");
        CacheManager cacheManager=database.cacheManager();
        FileManager fileManager=database.fileManager();
        fileManager.appendNewFileBlock("block");
        fileManager.appendNewFileBlock("block");

        Cache c=cacheManager.pin(new BlockId("block",1));
        Page content = c.getContent();
        //content.setInt(4,4);
        int i=content.getInt(4);
        System.out.println(i);
        content.setInt(4,i+1);
        c.modify();
        cacheManager.flushAll();
    }

    public static void testFile(){
        FileManager fileManager=new FileManager("testDB");
        fileManager.appendNewFileBlock("block");
        fileManager.appendNewFileBlock("block");
        fileManager.appendNewFileBlock("block");
        fileManager.appendNewFileBlock("block");
    }

    public static void testPin(){

        FileManager fileManager=new FileManager("testDB");

        LogManager logManager=new LogManager(fileManager);
        CacheManager cacheManager=new CacheManager(fileManager,logManager,8);
        Cache cache0=cacheManager.pin(new BlockId("block",0));
        Cache cache1=cacheManager.pin(new BlockId("block",1));
        Cache cache2=cacheManager.pin(new BlockId("block",2));
        Cache cache3=cacheManager.pin(new BlockId("block",3));
        Cache cache4=cacheManager.pin(new BlockId("block",4));
        Cache cache5=cacheManager.pin(new BlockId("block",5));
        Cache cache6=cacheManager.pin(new BlockId("block",6));
        Cache cache7=cacheManager.pin(new BlockId("block",7));

        cacheManager.pin(new BlockId("block",0));
        cacheManager.pin(new BlockId("block",1));

        cacheManager.unpin(cache0);
        cacheManager.unpin(cache2);
        cacheManager.unpin(cache0);


        Cache cache8=cacheManager.pin(new BlockId("block",8));
        Cache cache9=cacheManager.pin(new BlockId("block",9));

        cacheManager.pin(new BlockId("block",3));

        cacheManager.pin(new BlockId("block",0));
    }
}
