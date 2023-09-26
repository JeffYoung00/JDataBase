package scan;

import materialize.HashBucket;
import materialize.TemporaryTable;
import predicate.Constant;
import predicate.Term;
import record.Layout;
import record.Schema;
import transaction.Transaction;

import java.util.List;

public class HashJoinScan implements Scan {

    Transaction transaction;

    List<TemporaryTable> leftTempList;
    List<TemporaryTable> rightTempList;
    int position;
    MultiBuffersProductScan currentProductScan;
    Layout rightLayout;
    int hashBits;

    String leftJoinField;
    String rightJoinField;

    public HashJoinScan(Transaction transaction, HashBucket hashBucket, Schema leftSchema,Schema rightSchema,int hashBits,
    String leftJoinField,String rightJoinField){
        this.transaction=transaction;
        this.leftTempList = hashBucket.getLastTemporaryTablesLeft();
        this.rightTempList = hashBucket.getLastTemporaryTablesRight();
        this.rightLayout=new Layout(rightSchema);
        this.hashBits =hashBits;
        this.leftJoinField=leftJoinField;
        this.rightJoinField=rightJoinField;
    }

    @Override
    public void beforeFirst() {
        position=0;
        useNextProduct();
    }

    private boolean useNextProduct(){
        if(position>=leftTempList.size()){
            return false;
        }
        currentProductScan.close();
        currentProductScan=new MultiBuffersProductScan(transaction,rightLayout,
                leftTempList.get(position).open(),rightTempList.get(position).getFileName(),1<<hashBits);
        position++;
        return true;
    }

    @Override
    public boolean hasNext() {
        do{
            if(!nextProductRecord()){
                return false;
            }
        }while(!currentProductScan.getValue(leftJoinField).equals(currentProductScan.getValue(rightJoinField)));
        return true;
    }

    private boolean nextProductRecord(){
        while(!currentProductScan.hasNext()){
            if(!useNextProduct()){
                return false;
            }
        }
        return true;
    }

    @Override
    public Constant<?> getValue(String fieldName) {
        return currentProductScan.getValue(fieldName);
    }

    @Override
    public boolean hasField(String fieldName) {
        return currentProductScan.hasField(fieldName);
    }

    @Override
    public void close() {
        currentProductScan.close();
    }
}
