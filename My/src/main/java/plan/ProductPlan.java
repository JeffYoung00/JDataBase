package plan;

import record.Schema;
import scan.ProductScan;
import scan.Scan;

/**
 * 被multi product plan代替
 */
public class ProductPlan implements Plan{
    private Plan leftPlan,rightPlan;
    private Schema schema;

    public ProductPlan(Plan leftPlan,Plan rightPlan){
        this.leftPlan=leftPlan;
        this.rightPlan=rightPlan;

        schema=new Schema();
        schema.addAllFieldsIn(leftPlan.getSchema());
        schema.addAllFieldsIn(rightPlan.getSchema());
    }

    @Override
    public Scan open() {
        Scan leftScan= leftPlan.open();
        Scan rightScan= rightPlan.open();
        return new ProductScan(leftScan,rightScan);
    }

    @Override
    public int getBlockAccessedNumber() {
        return leftPlan.getRecordNumber()*rightPlan.getBlockAccessedNumber()+leftPlan.getBlockAccessedNumber();
    }

    @Override
    public int getRecordNumber() {
        return leftPlan.getRecordNumber()*rightPlan.getRecordNumber();
    }

    @Override
    public int getFieldDistinctValues(String fieldName) {
        if(leftPlan.getSchema().hasField(fieldName)){
            return leftPlan.getFieldDistinctValues(fieldName);
        }else {
            return rightPlan.getFieldDistinctValues(fieldName);
        }
    }

    @Override
    public int cost() {
        return leftPlan.cost()+rightPlan.cost();
    }

    @Override
    public Schema getSchema(){
        return schema;
    }
}
