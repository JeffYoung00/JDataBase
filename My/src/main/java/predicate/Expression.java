package predicate;

import lombok.ToString;
import scan.Scan;
@ToString
public class Expression {
    private String tableName=null;
    private String fieldName=null;
    private Constant<?> constantValue=null;
    public  Expression(String fieldName){
        this.fieldName=fieldName;
    }
    public Expression(String tableName,String fieldName){
        this.tableName=tableName;
        this.fieldName=fieldName;
    }
    public Expression(Constant<?> constantValue){
        this.constantValue=constantValue;
    }

    public Constant<?> getValue(Scan scan){
        if(constantValue!=null){
            return constantValue;
        }else{
            return scan.getValue(fieldName);
        }
    }

    public boolean isConstantValue(){
        return fieldName==null;
    }

    public String asFieldName(){
        return fieldName;
    }

    public void addTableName(String tableName){
        this.tableName=tableName;
    }

    public String asTableName(){
        return tableName;
    }

    public Object asConstantValue(){
        return constantValue.getValue();
    }

    public Constant<?> getConstant(){
        return constantValue;
    }

}
