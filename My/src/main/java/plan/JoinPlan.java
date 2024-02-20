package plan;

import file.FileManager;
import record.Layout;
import record.RecordPage;
import record.Schema;
import scan.Scan;
import transaction.Transaction;

/**
 * hash右表作为驱动表,merge随意,更小;index索引表作为右表
 * 认为小表是满射
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

        calculateMaterial();
    }

    /**
     * 计算,如果materialize之后,左右的Block()
     */
    protected void calculateMaterial(){
        int leftRPB = RecordPage.recordPerBlock(leftPlan.getSchema());
        int rightRPB= RecordPage.recordPerBlock(rightPlan.getSchema());
        leftBlock= leftPlan.getRecordNumber()/leftRPB;
        rightBlock=rightPlan.getBlockAccessedNumber()/rightRPB;
    }


    /**
     * 如果左边更小,交换左右
     */
    protected void compareAndSwapRight(){
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

    /**
     * 假设满射,小表中的数据有1/max(V1,V2)的概率成功映射
     * 即总record数/max(V1,V2)
     */
    @Override
    public int getRecordNumber() {
        return leftPlan.getRecordNumber()*rightPlan.getRecordNumber()/
                Math.max(leftPlan.getFieldDistinctValues(leftJoinFieldName),rightPlan.getFieldDistinctValues(rightJoinFieldName));
    }

    /**
     * 更大的一方distinct value会减少,只有部分被映射
     */
    @Override
    public int getFieldDistinctValues(String fieldName) {
        double leftSize=leftPlan.getFieldDistinctValues(leftJoinFieldName);
        double rightSize=rightPlan.getFieldDistinctValues(rightJoinFieldName);
        boolean leftBigger=leftSize>rightSize;
        double rate=leftBigger?leftSize/rightSize:rightSize/leftSize;

        if(leftPlan.getSchema().hasField(fieldName)){
            if(leftBigger){
                return (int)(leftPlan.getFieldDistinctValues(fieldName)/rate);
            }else{
                return leftPlan.getFieldDistinctValues(fieldName);
            }
        }else {
            if(leftBigger){
                return rightPlan.getFieldDistinctValues(fieldName);
            }else{
                return (int)(rightPlan.getFieldDistinctValues(fieldName)/rate);
            }
        }
    }

    @Override
    public Schema getSchema() {
        return schema;
    }
}
