package plan;

import record.Schema;
import record.Field;
import scan.ProjectScan;
import scan.Scan;

import java.util.List;
import java.util.stream.Collectors;

public class ProjectPlan implements Plan{
    private Plan plan;
    private Schema schema;

    public ProjectPlan(Plan plan, List<String> fieldNames){
        this.plan=plan;

        schema=new Schema();
        for(Field field: plan.getSchema().getFieldList()){
            if(fieldNames.contains(field.getName())){
                schema.addField(field);
            }
        }
    }

    @Override
    public Scan open() {
        Scan scan=plan.open();
        return new ProjectScan(scan,schema.getFieldList().stream().map(Field::getName).collect(Collectors.toList()));
    }

    @Override
    public int getBlockAccessedNumber() {
        return plan.getBlockAccessedNumber();
    }

    @Override
    public int getRecordNumber() {
        return plan.getRecordNumber();
    }

    @Override
    public int getFieldDistinctValues(String fieldName) {
        //如果不在fieldList中,本身用到这个函数也没有意义
        return plan.getFieldDistinctValues(fieldName);
    }

    @Override
    public int cost() {
        return plan.cost();
    }

    @Override
    public Schema getSchema(){
        return schema;
    }
}
