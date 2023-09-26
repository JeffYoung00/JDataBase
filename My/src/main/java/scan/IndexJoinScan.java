package scan;

import index.IndexScan;
import predicate.Constant;
import transaction.Transaction;

public class IndexJoinScan implements Scan{

    private Transaction transaction;
    private Scan leftScan;
    private IndexScan indexScan;
    private String leftJoinFieldName;

    public IndexJoinScan(Transaction transaction, Scan leftScan, IndexScan indexScan,String leftJoinFieldName){
        this.transaction=transaction;
        this.leftScan=leftScan;
        this.indexScan=indexScan;
        this.leftJoinFieldName=leftJoinFieldName;
    }

    @Override
    public void beforeFirst() {
        leftScan.beforeFirst();
    }

    @Override
    public boolean hasNext() {
        if(indexScan.hasNext()){
            return true;
        }
        while(leftScan.hasNext()){
            indexScan.beforeFirst(leftScan.getValue(leftJoinFieldName));
            if(indexScan.hasNext()){
               return true;
            }
        }
        return false;
    }

    @Override
    public Constant<?> getValue(String fieldName) {
        if(leftScan.hasField(fieldName)){
            return leftScan.getValue(fieldName);
        }else{
            return indexScan.getValue(fieldName);
        }
    }

    @Override
    public boolean hasField(String fieldName) {
        return leftScan.hasField(fieldName)|| indexScan.hasField(fieldName);
    }

    @Override
    public void close() {
        leftScan.close();
        indexScan.close();
    }
}
