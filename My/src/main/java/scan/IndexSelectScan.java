package scan;

import index.IndexScan;
import predicate.Constant;
import predicate.Term;
import transaction.Transaction;

public class IndexSelectScan implements Scan{

    private Transaction transaction;
    private IndexScan indexScan;
    private Term term;

    public IndexSelectScan(Transaction transaction, IndexScan indexScan, Term term){
        this.transaction=transaction;
        this.indexScan=indexScan;
        this.term=term;
    }

    @Override
    public void beforeFirst() {
        indexScan.beforeFirst(term.getRightExpression().getConstant());
    }

    @Override
    public boolean hasNext() {
        return indexScan.hasNext();
    }

    @Override
    public Constant<?> getValue(String fieldName) {
        return indexScan.getValue(fieldName);
    }

    @Override
    public boolean hasField(String fieldName) {
        return indexScan.hasField(fieldName);
    }

    @Override
    public void close() {
        indexScan.close();
    }
}
