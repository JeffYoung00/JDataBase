package plan;

import file.FileManager;
import record.Layout;
import record.RecordPage;
import record.Schema;
import scan.MultiBuffersProductScan;
import scan.Scan;
import transaction.Transaction;

/**
 * todo
 * 现在默认右表用一次materialize,左表不用
 */
public class MultipleProductPlan implements Plan{

    private Plan leftPlan;
    private Plan rightPlan;

    private boolean materializeRight =false;

    //右表大小
    private int leftBlock;
    private int rightBlock;

    private Schema schema;

    private Transaction transaction;

    private int bufferSize;

    public MultipleProductPlan(Transaction transaction,Plan leftPlan, Plan rightPlan, int bufferAvailable){
        this.leftPlan=leftPlan;
        this.rightPlan=rightPlan;
        this.transaction=transaction;

        compareLeftAndRight();

        this.bufferSize=Utils.findFactor(rightBlock,bufferAvailable);

        this.schema=new Schema();
        schema.addAllFieldsIn(leftPlan.getSchema());
        schema.addAllFieldsIn(rightPlan.getSchema());
    }

    private void compareLeftAndRight(){
        //计算,如果materialize之后,左右的Block()
        int leftRPB= RecordPage.recordPerBlock(leftPlan.getSchema());
        int rightRPB=RecordPage.recordPerBlock(rightPlan.getSchema());
        leftBlock= leftPlan.getRecordNumber()/leftRPB;
        rightBlock=rightPlan.getBlockAccessedNumber()/rightRPB;
        //如果左边更小,交换左右
        if(leftBlock<rightBlock){
            Plan temp=leftPlan;
            leftPlan=rightPlan;
            rightPlan=temp;

            int b=leftBlock;
            leftBlock=rightBlock;
            rightBlock=b;
        }

        if(rightBlock<rightPlan.getBlockAccessedNumber()){
            materializeRight =true;
        }
    }


    @Override
    public Scan open() {
        return new MultiBuffersProductScan(transaction,new Layout(rightPlan.getSchema()),leftPlan.open(),
                new MaterializePlan(rightPlan, transaction).openAsFile(),bufferSize);
    }

    @Override
    public int getBlockAccessedNumber() {
        int runs=Utils.calculateRunsByFactor(rightBlock,bufferSize);
        return leftPlan.getBlockAccessedNumber()*runs+ rightBlock;
    }

    public int cost(){
        int ret=0;
        if(materializeRight){
            ret+=rightPlan.getBlockAccessedNumber();
            ret+=rightBlock;
        }
        return ret;
    }

    @Override
    public int getRecordNumber() {
        return leftPlan.getRecordNumber()* rightPlan.getRecordNumber();
    }

    @Override
    public int getFieldDistinctValues(String fieldName) {
        if(leftPlan.getSchema().hasField(fieldName)){
            return leftPlan.getFieldDistinctValues(fieldName);
        }else{
            return rightPlan.getFieldDistinctValues(fieldName);
        }
    }

    @Override
    public Schema getSchema() {
        return schema;
    }
}
