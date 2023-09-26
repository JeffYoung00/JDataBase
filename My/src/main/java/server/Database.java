package server;

import cache.CacheManager;
import file.FileManager;
import log.LogManager;
import metadata.MetadataManager;
import metadata.TableManager;
import metadata.TableStatisticsManager;
import parse.Semantics;
import planner.*;
import transaction.recovery.RecoveryManager;
import transaction.Transaction;

import java.io.File;
import java.sql.Connection;

public class Database {

    public static String logFileName="log.log";
    public static String tableCatalogName="tableCatalog";
    public static String fieldCatalogName="fieldCatalog";
    public static String indexCatalogName="indexCatalog";

    public static String tablePostfix=".tbl";
    public static String tempTablePostfix=".tmp.tbl";
    public static String bTreeIndexPostfix =".bidx";

    public static String hashIndexPostfix =".hidx";
    public static String hashBucketPostfix =".hbkt";


    public static int BTree_Index=0;
    public static int Hash_Index=1;

    public static String urlPrefix="jdbc:jeffSql://";

    public static int Cache_SIZE=80;
    public static int Isolation_Level= Connection.TRANSACTION_REPEATABLE_READ;

    public static double tableStatisticsUpdateRate=1.5;


    FileManager fileManager;
    LogManager logManager;
    CacheManager cacheManager;
    RecoveryManager recoveryManager;
    Planner planner;
    MetadataManager metadataManager;

    public Database(String databaseDirName){
        File databaseDir=new File(databaseDirName);
        fileManager=new FileManager(databaseDirName);

        boolean isNewDatabase=!databaseDir.exists();
        if(isNewDatabase){
            databaseDir.mkdir();
        }

        logManager=new LogManager(fileManager);
        cacheManager=new CacheManager(fileManager,logManager,Cache_SIZE);
        recoveryManager=new RecoveryManager(fileManager,logManager,cacheManager);

        Transaction systemTransaction=new Transaction(fileManager,cacheManager,logManager);

        if(!isNewDatabase){
            recoveryManager.doRecover(systemTransaction);
        }

        metadataManager = new MetadataManager(systemTransaction);
        Semantics semantics=new Semantics(metadataManager);
        planner=new Planner(metadataManager,cacheManager,semantics);

        systemTransaction.commit();
    }

    /**
     * 正常关闭,flush all
     */
    public void flushCache(){
        cacheManager.flushAll();
    }

    public void flushLog(){
        logManager.flushAll();
    }

    public void  normalShutdown(){
        if(Transaction.getRunningTransactions().length!=0){
            System.out.println("not ready to shutdown");
            return;
        }
        logManager.flushAll();
        cacheManager.flushAll();
        fileManager.emptyFile(logFileName);
    }

    public Transaction newTransaction(){
        return new Transaction(fileManager,cacheManager,logManager);
    }

    public Planner planner(){
        return planner;
    }

    //debug用的方法
    public FileManager fileManager(){
        return fileManager;
    }
    public CacheManager cacheManager(){
        return cacheManager;
    }
    public LogManager logManager(){
        return logManager;
    }
    public MetadataManager metadataManager(){
        return metadataManager;
    }
}
