package test;

import cache.CacheManager;
import file.BlockId;
import file.FileManager;
import log.LogManager;
import metadata.MetadataManager;
import parse.data.CreateTableData;
import predicate.Constant;
import record.Field;
import record.Layout;
import record.Schema;
import record.TableScan;
import server.Database;
import transaction.Transaction;
import transaction.recovery.RecoveryManager;

import java.util.Arrays;

public class RecordTest {

    static FileManager fileManager = new FileManager("testDB");
    static LogManager logManager = new LogManager(fileManager);
    static CacheManager cacheManager = new CacheManager(fileManager, logManager, 8);
    static RecoveryManager recoveryManager = new RecoveryManager(fileManager, logManager, cacheManager);
    static Transaction systemTransaction=new Transaction(fileManager,cacheManager,logManager);
    static BlockId blockId0=new BlockId("block",0);
    static MetadataManager  metadataManager=new MetadataManager(systemTransaction);

    public static void main(String[] args) {
        recoveryManager.doRecover(systemTransaction);
        systemTransaction.commit();

        createTable();
//        readTable();
//        createTableMetadata();


        logManager.flushAll();
        cacheManager.flushAll();
        fileManager.emptyFile(Database.logFileName);

    }

    public static void readTable(){

        Transaction transaction=new Transaction(fileManager,cacheManager,logManager);
        Field field1=new Field("name",Field.String,20);
        Field field2=new Field("age",Field.Integer);
        Field field3=new Field("interest",Field.String,20);
        Schema schema=new Schema(Arrays.asList(field1,field2,field3));
        Layout layout=new Layout(schema);
        TableScan tableScan=new TableScan(transaction,layout,"testTable0");

        for(int i=0;i<6;i++){
            tableScan.hasNext();
        }
        for(int i=0;i<5;i++){
            tableScan.hasNext();
            tableScan.delete();
        }
        tableScan.beforeFirst();
        while(tableScan.hasNext()){
            String name= (String) tableScan.getValue("name").getValue();
            Integer age= (Integer) tableScan.getValue("age").getValue();
            String interest=(String) tableScan.getValue("interest").getValue();
            System.out.println(name+age+interest);
        }
        tableScan.close();
    }

    public static void createTable(){

        Transaction transaction=new Transaction(fileManager,cacheManager,logManager);

        Field field1=new Field("name",Field.String,20);
        Field field2=new Field("age",Field.Integer);
        Field field3=new Field("interest",Field.String,20);
        Schema schema=new Schema(Arrays.asList(field1,field2,field3));
        Layout layout=new Layout(schema);
        TableScan tableScan=new TableScan(transaction,layout,"testtable0");
        tableScan.insert();
        tableScan.setValue("name",new Constant<>("jeff"));
        tableScan.setValue("age",new Constant<>(21));
        tableScan.setValue("interest",new Constant<>("computer"));
        for(int i=0;i<30;i++){
            tableScan.insert();
            tableScan.setValue("name",new Constant<>("test"+i));
            tableScan.setValue("age",new Constant<>(21+i));
            tableScan.setValue("interest",new Constant<>("computer"));
        }
        tableScan.close();
        transaction.commit();


    }

    public static void createTableMetadata(){
        Field field1=new Field("name",Field.String,20);
        Field field2=new Field("age",Field.Integer);
        Field field3=new Field("interest",Field.String,20);
        Schema schema=new Schema(Arrays.asList(field1,field2,field3));

        Transaction transaction=new Transaction(fileManager,cacheManager,logManager);
        metadataManager.createTable(transaction,new CreateTableData("testtable0",schema));

        TableScan tableScan=new TableScan(transaction,metadataManager.getTableLayout("testtable0"),"testtable0");
        while(tableScan.hasNext()){
            String name= (String) tableScan.getValue("name").getValue();
            Integer age= (Integer) tableScan.getValue("age").getValue();
            String interest=(String) tableScan.getValue("interest").getValue();
            System.out.println(name+age+interest);
        }
        tableScan.close();
        transaction.commit();

    }
}
