package plan;

import file.FileManager;
import record.Layout;
import record.RecordPage;
import record.Schema;
import scan.Scan;
import transaction.Transaction;

/**
 * 尽量lazy
 */
public abstract class JoinPlan implements Plan{

    protected Plan leftPlan;
    protected Plan rightPlan;

    protected String leftJoinFieldName;
    protected String rightJoinFieldName;

    protected int leftBlock;
    protected int rightBlock;

    protected Schema schema;
    protected Transaction transaction;

    public JoinPlan(Transaction transaction,Plan leftPlan, Plan rightPlan, String leftJoinFieldName, String rightJoinFieldName){
        this.leftPlan=leftPlan;
        this.rightPlan=rightPlan;
        this.leftJoinFieldName=leftJoinFieldName;
        this.rightJoinFieldName=rightJoinFieldName;
        this.transaction=transaction;

        this.schema=new Schema();
        schema.addAllFieldsIn(leftPlan.getSchema());
        schema.addAllFieldsIn(rightPlan.getSchema());
    }

    protected void calculateMaterial(){
        //计算,如果materialize之后,左右的Block()
        int leftRPB = RecordPage.recordPerBlock(leftPlan.getSchema());
        int rightRPB= RecordPage.recordPerBlock(rightPlan.getSchema());
        leftBlock= leftPlan.getRecordNumber()/leftRPB;
        rightBlock=rightPlan.getBlockAccessedNumber()/rightRPB;
    }


    protected void compareAndSwapRight(){
        //如果左边更小,交换左右
        if(leftBlock<rightBlock){
            Plan temp=leftPlan;
            leftPlan=rightPlan;
            rightPlan=temp;

            int b=leftBlock;
            leftBlock=rightBlock;
            rightBlock=b;

            String s=leftJoinFieldName;
            leftJoinFieldName=rightJoinFieldName;
            rightJoinFieldName=s;
        }
    }

    @Override
    public abstract Scan open() ;

    //
    @Override
    public abstract int getBlockAccessedNumber() ;

    /**
     * 假设满射,小表中的数据有1/max(V1,V2)的概率成功映射
     * 即总record数/max(V1,V2)
     */
    @Override
    public int getRecordNumber() {
        return leftPlan.getRecordNumber()*rightPlan.getRecordNumber()/
                Math.max(leftPlan.getFieldDistinctValues(leftJoinFieldName),rightPlan.getFieldDistinctValues(rightJoinFieldName));
    }

    //todo 0

    /**
     * 更大的一方distinct value会减少,只有部分被映射
     */
    @Override
    public int getFieldDistinctValues(String fieldName) {
        int leftSize=leftPlan.getFieldDistinctValues(leftJoinFieldName);
        int rightSize=rightPlan.getFieldDistinctValues(rightJoinFieldName);
        boolean leftBigger=leftSize>rightSize;
        int rate=leftBigger?leftSize/rightSize:rightSize/leftSize;

        if(leftPlan.getSchema().hasField(fieldName)){
            if(leftBigger){
                return leftPlan.getFieldDistinctValues(fieldName)/rate;
            }else{
                return leftPlan.getFieldDistinctValues(fieldName);
            }
        }else {
            if(leftBigger){
                return rightPlan.getFieldDistinctValues(fieldName);
            }else{
                return rightPlan.getFieldDistinctValues(fieldName)/rate;
            }

        }
    }

    @Override
    public Schema getSchema() {
        return schema;
    }
}
