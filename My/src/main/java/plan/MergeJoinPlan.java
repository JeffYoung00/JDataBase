package plan;

import file.FileManager;
import materialize.MergeSort;
import record.Field;
import record.Layout;
import record.TableScan;
import scan.MergeJoinScan;
import scan.Scan;
import transaction.Transaction;

public class MergeJoinPlan extends JoinPlan{

    int availableBuffer;
    int leftRuns;
    int rightRuns;

    int leftRoute;
    int rightRoute;

    public MergeJoinPlan(Transaction transaction,Plan leftPlan, Plan rightPlan, String leftJoinFieldName, String rightJoinFieldName, int availableBuffer) {
        super(transaction,leftPlan, rightPlan, leftJoinFieldName, rightJoinFieldName);
        calculateMaterial();

        this.availableBuffer=availableBuffer;

        this.leftRoute=Utils.findRoot(leftPlan.getBlockAccessedNumber(),availableBuffer);
        this.rightRoute=Utils.findRoot(rightPlan.getBlockAccessedNumber(),availableBuffer);

        this.leftRuns=Utils.calculateRunsByRoot(leftPlan.getBlockAccessedNumber(),availableBuffer);
        this.rightRuns=Utils.calculateRunsByRoot(rightPlan.getBlockAccessedNumber(),availableBuffer);
    }


    @Override
    public Scan open() {

        TableScan left=new MergeSort<>(transaction, leftPlan.getSchema(), leftJoinFieldName, leftPlan.open(), leftRoute).getLastTableScan();
        TableScan right=new MergeSort<>(transaction,rightPlan.getSchema(),rightJoinFieldName,rightPlan.open(),rightRoute).getLastTableScan();

        return new MergeJoinScan(left,right,leftJoinFieldName,rightJoinFieldName);
    }

    @Override
    public int getBlockAccessedNumber() {
        return leftBlock+rightBlock;
    }

    public int cost(){
        return leftPlan.getBlockAccessedNumber()+rightPlan.getBlockAccessedNumber()+(2*leftRuns-1)*leftBlock+(2*rightRuns-1)*rightBlock
                +leftPlan.cost()+rightPlan.cost();
    }
}
