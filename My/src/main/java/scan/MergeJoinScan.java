package scan;

import predicate.Constant;
import record.RecordId;
import record.TableScan;

public class MergeJoinScan implements Scan{
    
    TableScan scan1;
    TableScan scan2;
    String fieldName1;
    String fieldName2;
    
    Constant<?> joinValue=null;
    RecordId savePosition=null;
    
    public MergeJoinScan(TableScan scan1, TableScan scan2, String fieldName1, String fieldName2){
        this.scan1=scan1;
        this.scan2=scan2;
        this.fieldName1=fieldName1;
        this.fieldName2=fieldName2;
    }

    @Override
    public void beforeFirst() {
        scan1.beforeFirst();
        scan2.beforeFirst();
    }

    @Override
    public boolean hasNext() {
        boolean hasmore2 = scan2.hasNext();
        if (hasmore2 && scan2.getValue(fieldName2).equals(joinValue))
            return true;

        boolean hasmore1 = scan1.hasNext();
        if (hasmore1 && scan1.getValue(fieldName1).equals(joinValue)) {
            scan2.moveToRecordId(savePosition);
            return true;
        }

        while (hasmore1 && hasmore2) {
            Constant<?> v1 = scan1.getValue(fieldName1);
            Constant<?> v2 = scan2.getValue(fieldName2);
            if (v1.compareTo(v2) < 0)
                hasmore1 = scan1.hasNext();
            else if (v1.compareTo(v2) > 0)
                hasmore2 = scan2.hasNext();
            else {
                savePosition=scan2.getRecordId();
                joinValue  = scan2.getValue(fieldName2);
                return true;
            }
        }
        return false;
    }

    @Override
    public Constant<?> getValue(String fieldName) {
        if(scan1.hasField(fieldName)){
            return scan1.getValue(fieldName);
        }else{
            return scan2.getValue(fieldName);
        }
    }

    @Override
    public boolean hasField(String fieldName) {
        return scan1.hasField(fieldName)||scan2.hasField(fieldName);
    }

    @Override
    public void close() {
        scan1.close();
        scan2.close();
    }
}
