package scan;

import predicate.Constant;

public class ProductScan implements Scan{

    Scan leftScan;
    Scan rightScan;
    boolean emptyLeft=false;

    public ProductScan(Scan leftScan,Scan rightScan){
        this.leftScan=leftScan;
        this.rightScan=rightScan;
    }

    @Override
    public void beforeFirst() {
        leftScan.beforeFirst();
        //左表为空
        if(leftScan.hasNext()){
            emptyLeft=true;
        }
        rightScan.beforeFirst();
    }

    @Override
    public boolean hasNext() {
        if(emptyLeft){
            return false;
        }
        if(rightScan.hasNext()){
            return true;
        }
        rightScan.beforeFirst();
        if(leftScan.hasNext()&& rightScan.hasNext()){
            return true;
        }
        return false;
    }

    @Override
    public Constant<?> getValue(String fieldName) {
        if(leftScan.hasField(fieldName)){
            return leftScan.getValue(fieldName);
        } else{
            return rightScan.getValue(fieldName);
        }
    }

    @Override
    public boolean hasField(String fieldName) {
        return leftScan.hasField(fieldName)|| rightScan.hasField(fieldName);
    }

    @Override
    public void close() {
        leftScan.close();
        rightScan.close();
    }
}
