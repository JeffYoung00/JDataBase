package plan;

import record.Schema;
import predicate.Predicate;
import scan.Scan;
import scan.SelectScan;

public class SelectPlan implements Plan {
    private Plan plan;
    private Predicate predicate;

    public SelectPlan(Plan plan,Predicate predicate){
        this.plan=plan;
        this.predicate=predicate;
    }

    @Override
    public Scan open() {
        return new SelectScan(plan.open(),predicate);
    }

    @Override
    public int getBlockAccessedNumber() {
        return plan.getBlockAccessedNumber();
    }

    @Override
    public int getRecordNumber() {
        return plan.getRecordNumber()/predicate.reductionFactor(plan);
    }

    @Override
    public int getFieldDistinctValues(String fieldName) {
        return plan.getFieldDistinctValues(fieldName)/predicate.reductionFactor(plan);
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
