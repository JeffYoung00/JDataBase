package plan;

import index.IndexScan;
import index.bplustree.BTreeIndexScan;
import metadata.IndexInfo;
import record.Schema;
import predicate.Term;
import record.TableScan;
import scan.IndexSelectScan;
import scan.Scan;
import transaction.Transaction;

public class IndexSelectPlan implements Plan{

    private Term term;
    private IndexInfo indexInfo;
    private Plan plan;

    private Transaction transaction;

    public IndexSelectPlan(Plan plan,Transaction transaction ,Term term, IndexInfo indexInfo){
        this.plan=plan;
        this.term=term;
        this.indexInfo=indexInfo;

        this.transaction=transaction;
    }

    @Override
    public Scan open() {
        TableScan open = (TableScan) plan.open();
        IndexScan indexScan=new BTreeIndexScan(transaction,indexInfo,open);
        return new IndexSelectScan(transaction,indexScan,term);
    }

    @Override
    public int getBlockAccessedNumber(){
        return getRecordNumber()+indexInfo.getIndexHeight();
    }

    @Override
    public int getRecordNumber() {
        return plan.getRecordNumber()/term.reductionFactor(plan);
    }

    @Override
    public int getFieldDistinctValues(String fieldName) {
        return plan.getFieldDistinctValues(fieldName)/term.reductionFactor(plan);
    }

    @Override
    public int cost() {
        return plan.cost();
    }

    @Override
    public Schema getSchema() {
        return plan.getSchema();
    }
}
