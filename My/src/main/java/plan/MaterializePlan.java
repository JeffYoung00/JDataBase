package plan;

import materialize.TemporaryTable;
import record.Layout;
import record.RecordPage;
import record.Schema;
import record.TableScan;
import scan.Scan;
import transaction.Transaction;

public class MaterializePlan implements Plan{

    private Plan plan;
    private Transaction transaction;

    public MaterializePlan (Plan plan, Transaction transaction){
        this.plan=plan;
        this.transaction=transaction;
    }

    private TemporaryTable temporaryTable;

    @Override
    public Scan open() {
        Scan scan=plan.open();
        Layout layout=new Layout(plan.getSchema());
        temporaryTable=new TemporaryTable(transaction,layout);
        TableScan temp = temporaryTable.open();
        while(scan.hasNext()) {
            for (String field : layout.getFieldSet()) {
                temp.setValue(field, scan.getValue(field));
            }
        }
        temp.beforeFirst();
        scan.close();
        return temp;
    }

    public String openAsFile(){
        open().close();
        return temporaryTable.getFileName();
    }

    @Override
    public int getBlockAccessedNumber() {
        return plan.getRecordNumber()/ RecordPage.recordPerBlock(plan.getSchema());
    }

    @Override
    public int getRecordNumber() {
        return plan.getRecordNumber();
    }

    @Override
    public int getFieldDistinctValues(String fieldName) {
        return plan.getFieldDistinctValues(fieldName);
    }

    @Override
    public int cost() {
        return getBlockAccessedNumber()+plan.getBlockAccessedNumber()+ plan.cost();
    }

    @Override
    public Schema getSchema() {
        return plan.getSchema();
    }
}
