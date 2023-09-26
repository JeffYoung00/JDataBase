package metadata;

import parse.data.CreateIndexData;
import parse.data.CreateTableData;
import record.Field;
import record.Layout;
import record.Schema;
import transaction.Transaction;

import java.util.Map;
import java.util.Set;

public class MetadataManager {

    private TableManager tableManager;
    private IndexManager indexManager;
    private TableStatisticsManager tableStatisticsManager;

    public MetadataManager(Transaction transaction){
        this.tableManager=new TableManager(transaction);
        this.indexManager=new IndexManager(transaction,tableManager);
        this.tableStatisticsManager=new TableStatisticsManager(transaction,tableManager);
    }

    //create drop
    public void createTable(Transaction transaction, CreateTableData createTableData){
        tableManager.createTable(transaction,createTableData.getSchema(),createTableData.getTableName());
    }

    public void dropTable(Transaction transaction,String tableName){
        tableManager.dropTable(transaction,tableName);
    }

    public void createIndex(Transaction transaction, CreateIndexData createIndexData){
        indexManager.createIndex(transaction,createIndexData);
    }

    public void dropIndex(Transaction transaction,String indexName){
        indexManager.dropIndex(transaction,indexName);
    }

    //table info

    public boolean hasTable(String table){
        return tableManager.hasTable(table);
    }

    public boolean hasField(String tableName,String fieldName){
        return tableManager.hasField(tableName,fieldName);
    }

    public Layout getTableLayout(String tableName){
        return tableManager.getLayout(tableName);
    }

    public Schema getTableSchema(String tableName){
        return tableManager.getSchema(tableName);
    }

    public Set<String> getTableSet(){
        return tableManager.getTableSet();
    }

    public Field getFieldInfo(String tableName, String fieldName){
        return tableManager.getFieldInfo(tableName,fieldName);
    }

    //index info

    public int getIndexHeight(String tableName,String fieldName){
        return indexManager.getIndexHeight(tableName,fieldName);
    }

    public boolean hasIndex(String tableName,String fieldName){
        return indexManager.getIndexHeight(tableName,fieldName)==-1;
    }

    public boolean hasIndex(String indexName){
        return indexManager.hasIndexByName(indexName);
    }

//    public List<IndexInfo> getTableIndexInfos(String tableName){
//        return indexManager.getTableIndexInfoList(tableName);
//    }

    public Map<String,IndexInfo> getTableIndexInfos(String tableName){
        return indexManager.getTableInfoMap(tableName);
    }

    //table statistics info

    public TableStatistics getTableStatistics(String tableName){
        return tableStatisticsManager.getTableStatistics(tableName);
    }

    //update statistics

    public void updateStatistics(Transaction transaction){
        tableStatisticsManager.updateStatistics(transaction);
    }
}
