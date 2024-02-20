package plan;

import index.IndexScan;
import index.bplustree.BTreeIndexScan;
import metadata.IndexInfo;
import metadata.TableStatistics;
import record.Schema;
import record.TableScan;
import scan.IndexJoinScan;
import scan.IndexSelectScan;
import scan.Scan;
import transaction.Transaction;

public class IndexJoinPlan extends JoinPlan{

    private IndexInfo indexInfo;

    public IndexJoinPlan(Transaction transaction,Plan leftPlan, Plan rightPlan, String leftJoinFieldName, IndexInfo indexInfo){
        super(transaction,leftPlan,rightPlan,leftJoinFieldName,indexInfo.getField().getName());
        this.indexInfo=indexInfo;
    }

    @Override
    public Scan open() {
        TableScan open = (TableScan) rightPlan.open();
        IndexScan indexScan=new BTreeIndexScan(transaction,indexInfo,open);

        Scan scan=leftPlan.open();
        return new IndexJoinScan(transaction,scan,indexScan,leftJoinFieldName);
    }

    //left plan遍历 +
    // 记录数*找索引+
    // 命中记录的回表
    @Override
    public int getBlockAccessedNumber() {
        return leftPlan.getBlockAccessedNumber()
                +leftPlan.getRecordNumber()*indexInfo.getIndexHeight()
                +getRecordNumber();
    }

    @Override
    public int cost() {
        return leftPlan.cost()+rightPlan.cost();
    }
}
