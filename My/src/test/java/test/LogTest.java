//package test;
//
//import file.FileManager;
//import file.Page;
//import log.LogManager;
//
//import java.util.Iterator;
//
//public class LogTest {
//    static FileManager fileManager=new FileManager("testDB");
//    static LogManager logManager=new LogManager(fileManager);
//
//
//
//    public static void main(String[] args) {
//
//        for(int i=0;i<70;i++){
//            byte[] a=new byte[8];
//            Page p=new Page(a);
//            p.setInt(0,i);
//            p.setInt(4,100+i);
//            logManager.newLogRecord(a);
//        }
//
//        printLogRecords();
//
//    }
//
//    private static void printLogRecords() {
//        System.out.println(" print ");
//        Iterator<byte[]> iter = logManager.iterator();
//        while (iter.hasNext()) {
//            byte[] rec = iter.next();
//            Page p = new Page(rec);
//            int v1=p.getInt(0);
//            int v2=p.getInt(4);
//            System.out.println("[" + v1 + ", " + v2 + "]");
//        }
//        System.out.println();
//    }
//
//}
