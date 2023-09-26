package metadata;

import parse.data.CreateIndexData;
import predicate.Constant;
import record.Field;
import record.Layout;
import record.Schema;
import record.TableScan;
import server.Database;
import transaction.Transaction;

import java.util.*;

public class IndexManager {

    //field和table的最长名字
    private static final int Max_Name_Length =32;


    private Layout indexCatalogLayout;


    //private Map<String,IndexInfo> indexInfoMap=new HashMap<>();

    //tableName->fieldName->index info
    private Map<String,Map<String,IndexInfo>> indexInfoMap=new HashMap<>();
    private TableManager tableManager;

    public IndexManager(Transaction transaction,TableManager tableManager){
        this.tableManager=tableManager;
        initIndexCatalogLayout();
        initIndexCatalog(transaction);
    }

    private void initIndexCatalogLayout(){
        Schema schema=new Schema();
        schema.addField(new Field("indexName",Field.String,Max_Name_Length));
        schema.addField(new Field("tableName",Field.String,Max_Name_Length));
        schema.addField(new Field("fieldName",Field.String,Max_Name_Length));
        schema.addField(new Field("indexHeight",Field.Integer));
        schema.addField(new Field("indexType",Field.Integer));
        indexCatalogLayout=new Layout(schema);
    }

    private void initIndexCatalog(Transaction transaction){

        for(String tableName:tableManager.getTableSet()){
            indexInfoMap.put(tableName,new HashMap<>());
        }

        TableScan tableScan=new TableScan(transaction,indexCatalogLayout, Database.indexCatalogName);
        while(tableScan.hasNext()){
            String indexName= (String) tableScan.getValue("indexName").getValue();
            String tableName=(String) tableScan.getValue("tableName").getValue();
            String fieldName=(String) tableScan.getValue("fieldName").getValue();
            Integer type=(Integer)tableScan.getValue("indexType").getValue();
            Integer height=(Integer)tableScan.getValue("indexHeight").getValue();
            Integer rootBlockNumber=(Integer)tableScan.getValue("rootBlockNumber").getValue();

            Field field = tableManager.getFieldInfo(tableName, fieldName);
            IndexInfo  indexInfo=new IndexInfo(indexName,field,type,height,rootBlockNumber);
            indexInfoMap.get(tableName).put(fieldName,indexInfo);
        }
        tableScan.close();
    }

    public void createIndex(Transaction transaction, CreateIndexData createIndexData) {

        TableScan tableScan = new TableScan(transaction, indexCatalogLayout, Database.indexCatalogName);
        tableScan.insert();
        tableScan.setValue("indexName", new Constant<>(createIndexData.getIndexName()));
        tableScan.setValue("tableName", new Constant<>(createIndexData.getTableName()));
        tableScan.setValue("fieldName", new Constant<>(createIndexData.getFieldName()));
        tableScan.setValue("indexType", new Constant<>(Database.BTree_Index));
        tableScan.setValue("indexHeight", new Constant<>(1));
        tableScan.setValue("rootBlockNumber", new Constant<>(0));
        tableScan.close();

        indexInfoMap.get(createIndexData.getTableName()).put(createIndexData.getFieldName(),
                new IndexInfo(createIndexData.getIndexName(),
                        tableManager.getFieldInfo(createIndexData.getTableName(), createIndexData.getFieldName()),
                        Database.BTree_Index, 1, 0));
    }

    public void dropIndex(Transaction transaction,String indexName){

        String tableName=null;
        String fieldName=null;
        for(Map.Entry<String,Map<String,IndexInfo>>mapEntry:indexInfoMap.entrySet()){
            for(Map.Entry<String,IndexInfo>infoEntry:mapEntry.getValue().entrySet()){
                if(infoEntry.getValue().getIndexName().equals(indexName)){
                    tableName= mapEntry.getKey();
                    fieldName= infoEntry.getKey();
                }
            }
        }

        if(tableName!=null&&fieldName!=null){
            indexInfoMap.get(tableName).remove(fieldName);
        }

        TableScan tableScan=new TableScan(transaction,indexCatalogLayout,Database.indexCatalogName);
        while(tableScan.hasNext()){
            if(tableScan.getValue("indexName").getValue().equals(indexName)){
                tableScan.delete();
                break;
            }
        }
        tableScan.close();
    }

    public int getIndexHeight(String tableName,String fieldName){
        IndexInfo info=indexInfoMap.get(tableName).get(fieldName);
        if(info==null){
            return -1;
        }else{
            return info.getIndexHeight();
        }
    }

    public boolean hasIndexByName(String indexName){
        for(Map<String,IndexInfo> map:indexInfoMap.values()){
            for(IndexInfo info:map.values()){
                if(info.getIndexName().equals(indexName)){
                    return true;
                }
            }
        }
        return false;
    }

    public Map<String,IndexInfo> getTableInfoMap(String tableName){
        return indexInfoMap.get(tableName);
    }

}
