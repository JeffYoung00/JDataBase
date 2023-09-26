package metadata;

import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import record.Layout;
import record.TableScan;
import transaction.Transaction;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TableStatisticsManager {

    private Map<String,TableStatistics> tableStatisticsMap=new ConcurrentHashMap<>();
    private TableManager tableManager;

    public TableStatisticsManager(Transaction transaction, TableManager tableManager){
        this.tableManager=tableManager;
        for(String s:tableManager.getTableSet()){
            TableStatistics statistics=readTableStatistics(transaction,s);
            tableStatisticsMap.put(s,statistics);
        }
    }

    private TableStatistics readTableStatistics(Transaction transaction,String tableName){
        Layout layout=tableManager.getLayout(tableName);
        Map<String,HyperLogLog> hyperLogLogMap=new HashMap<>(layout.getFieldSet().size());
        for(String s: layout.getFieldSet()){
            hyperLogLogMap.put(s,new HyperLogLog(10));
        }
        TableScan tableScan=new TableScan(transaction,layout,tableName);
        int recordNumber=0;
        while(tableScan.hasNext()){
            for(String s: layout.getFieldSet()){
                hyperLogLogMap.get(s).offer(tableScan.getValue(s).getValue());
            }
            recordNumber++;
        }
        int blockNumber=tableScan.getRecordId().getBlockNumber()+1;
        Map<String,Double> dinstinctMap=new HashMap<>(layout.getFieldSet().size());
        for(Map.Entry<String,HyperLogLog>entry:hyperLogLogMap.entrySet()){
            dinstinctMap.put( entry.getKey(), (double)entry.getValue().cardinality()/recordNumber);
        }
        return new TableStatistics(recordNumber,blockNumber,dinstinctMap);
    }

    public TableStatistics getTableStatistics(String tableName) {
        return tableStatisticsMap.get(tableName);
    }

    public void updateStatistics(Transaction transaction){
        for(Map.Entry<String,TableStatistics>entry:tableStatisticsMap.entrySet()){
            if(entry.getValue().decideReplace()){
                TableStatistics tableStatistics=readTableStatistics(transaction,entry.getKey());
                tableStatisticsMap.put(entry.getKey(),tableStatistics);
            }
        }
    }
}
