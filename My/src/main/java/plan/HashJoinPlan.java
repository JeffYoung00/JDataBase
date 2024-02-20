package plan;

import materialize.HashBucket;
import scan.HashJoinScan;
import scan.Scan;
import transaction.Transaction;

public class HashJoinPlan extends JoinPlan{

    //理想情况hash"runs"次就能很平均,最多额外hash两次
    private static int Extra_Hash=2;

    int availableBuffer;
    int runs;
    int hashBits;

    public HashJoinPlan(Transaction transaction,Plan leftPlan, Plan rightPlan, String leftJoinFieldName, String rightJoinFieldName, int availableBuffer){
        super(transaction,leftPlan,rightPlan,leftJoinFieldName,rightJoinFieldName);
        compareAndSwapRight();

        this.availableBuffer=availableBuffer;
        int root =Utils.findRoot2(rightBlock,availableBuffer);
        this.runs=Utils.calculateRunsByRoot(rightBlock, root);
        this.hashBits= (int) Math.log(root);
    }

    @Override
    public Scan open() {
        HashBucket hashBucket=new HashBucket(transaction,leftPlan.getSchema(),rightPlan.getSchema(),leftJoinFieldName,rightJoinFieldName,
                leftPlan.open(),rightPlan.open(),hashBits,runs+Extra_Hash);
        return new HashJoinScan(transaction,hashBucket,leftPlan.getSchema(),rightPlan.getSchema(),hashBits,leftJoinFieldName,rightJoinFieldName);
    }

    @Override
    public int getBlockAccessedNumber() {
        return leftBlock+rightBlock;
    }

    public int cost(){
        return leftPlan.getBlockAccessedNumber()+rightPlan.getBlockAccessedNumber()+(2*runs-1)*(leftBlock+rightBlock)
                +leftPlan.cost()+rightPlan.cost();
    }
}
