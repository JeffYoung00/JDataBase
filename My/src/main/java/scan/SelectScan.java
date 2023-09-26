package scan;

import predicate.Constant;
import predicate.Predicate;
import record.RecordId;

public class SelectScan implements UpdateScan{

    //todo 检查hasField的时间? 构造之后检查predicate
    //field name exception

    Scan scan;
    Predicate predicate;

    public SelectScan(Scan scan, Predicate predicate){
        this.scan=(UpdateScan) scan;
        this.predicate=predicate;
    }

    @Override
    public void beforeFirst() {
        scan.beforeFirst();
    }

    @Override
    public boolean hasNext() {
        while(scan.hasNext()){
            if(predicate.isSatisfied(scan)){
                return true;
            }
        }
        return false;
    }

    @Override
    public Constant<?> getValue(String fieldName) {
        return scan.getValue(fieldName);
    }

    @Override
    public boolean hasField(String fieldName) {
        return scan.hasField(fieldName);
    }

    @Override
    public void close() {
        scan.close();
    }

    @Override
    public void setValue(String fieldName, Constant<?> value) {
        UpdateScan updateScan=(UpdateScan) scan;
        updateScan.setValue(fieldName,value);
    }

    @Override
    public void insert() {
        UpdateScan updateScan=(UpdateScan) scan;
        updateScan.insert();
    }

    @Override
    public void delete() {
        UpdateScan updateScan=(UpdateScan) scan;
        updateScan.delete();
    }

    @Override
    public RecordId getRecordId() {
        UpdateScan updateScan=(UpdateScan) scan;
        return updateScan.getRecordId();
    }
}
