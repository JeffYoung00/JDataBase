package plan;

import metadata.TableStatistics;
import record.Schema;
import scan.Scan;
import record.Layout;
import record.TableScan;
import transaction.Transaction;

public class TablePlan implements Plan{

    private TableStatistics statistics;
    private Schema schema;

    //for open()
    private Layout layout;
    private Transaction transaction;
    private String tableName;

    public TablePlan(Transaction transaction, TableStatistics statistics,Layout layout,Schema schema,String tableName){
        this.statistics=statistics;
        this.schema=schema;
        this.transaction=transaction;
        this.layout=layout;
        this.tableName=tableName;
    }

    @Override
    public Scan open() {
        return new TableScan(transaction,layout,tableName);
    }

    @Override
    public int getBlockAccessedNumber() {
        return statistics.getBlockAccessedNumber();
    }

    @Override
    public int getRecordNumber() {
        return statistics.getRecordNumber();
    }

    @Override
    public int getFieldDistinctValues(String fieldName) {
        return statistics.getFieldDistinctValues(fieldName);
    }

    @Override
    public int cost() {
        return 0;
    }

    @Override
    public Schema getSchema() {
        return schema;
    }
}
