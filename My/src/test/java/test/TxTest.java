//package test;
//
//import cache.CacheManager;
//import file.BlockId;
//import file.FileManager;
//import file.Page;
//import log.LogManager;
//import transaction.Transaction;
//import transaction.recovery.RecoveryManager;
//import transaction.recovery.logrecord.*;
//
//import java.util.Arrays;
//import java.util.Iterator;
//
//public class TxTest {
//
//    static FileManager fileManager = new FileManager("testDB");
//    static LogManager logManager = new LogManager(fileManager);
//    static CacheManager cacheManager = new CacheManager(fileManager, logManager, 8);
//    static RecoveryManager recoveryManager = new RecoveryManager(fileManager, logManager, cacheManager);
//    static Transaction systemTransaction=new Transaction(fileManager,cacheManager,logManager);
//    static BlockId blockId0=new BlockId("block",0);
//
//    public static void main(String[] args) {
////        runFile();
//
////        run();
//
////        runLogFile();
////        runLogFile();
//
//        readLog();
////        runRecover();
//
//        end();
//
//    }
//
//    public static void end(){
//        systemTransaction.commit();
//    }
//
//    public static void run() {
//
//
//        //前面一次不flush memory,第二次recovery
//
//        recoveryManager.doRecover(systemTransaction);
//
//
//        Transaction transaction1 = new Transaction(fileManager, cacheManager, logManager);
//        BlockId blockId = new BlockId("block", 0);
//        transaction1.pin(new BlockId("block", 0));
//
//        //System.out.println(transaction1.getInt(blockId,0));
//        //System.out.println(transaction1.getInt(blockId,4));
//        //transaction1.setInt(blockId,4,64,true);
//
//        System.out.println(transaction1.getString(blockId, 100));
//        transaction1.setString(blockId, 100, "this is for test recovery", true);
//        System.out.println(transaction1.getString(blockId, 100));
//
//        transaction1.commit();
////        transaction1.commit();
//
//        logManager.flushAll();
////        cacheManager.flushAll();
//    }
//
//    public static void readLog() {
//
//
//        Iterator<byte[]> iterator = logManager.iterator();
//        Page page = new Page(null);
//        while (iterator.hasNext()) {
//            byte[] recordBytes = iterator.next();
////            System.out.println("len" + recordBytes.length);
//            page.setContent(recordBytes);
//            int type = page.getInt(0);
//            if (type == LogRecord.SET_INT) {
//                SetIntRecord r = new SetIntRecord(page);
//                System.out.println("rollback:" + r);
//            } else if (type == LogRecord.SET_STRING) {
//                SetStringRecord r = new SetStringRecord(page);
//                //debug
//                System.out.println("rollback:" + r);
//            } else {
//                System.out.println("rollback byte:" + Arrays.toString(recordBytes));
//            }
//        }
//    }
//
//    public static void runFile() {
//        fileManager.appendNewFileBlock("block");
//        fileManager.appendNewFileBlock("block");
//    }
//
//    public static void runRecover() {
//
//        //前面一次不flush memory,第二次recovery
//        recoveryManager.doRecover(systemTransaction);
//        systemTransaction.commit();
//
//        Transaction transaction1 = new Transaction(fileManager, cacheManager, logManager);
//        BlockId blockId = new BlockId("block", 0);
//        transaction1.pin(blockId);
//
//
//        System.out.println(transaction1.getString(blockId, 100));
//        transaction1.setString(blockId, 100, "this is for test recovery", true);
//        System.out.println(transaction1.getString(blockId, 100));
//
//        transaction1.commit();
//
//        logManager.flushAll();
////        cacheManager.flushAll();
//    }
//
//    public static void runLogFile(){
//        for(int i=0;i<30;i++){
//            systemTransaction.setString(blockId0,256,"this is log test"+i,true);
//        }
//        logManager.flushAll();
//
//    }
//}
//
//
