package parse.data;

import lombok.Getter;
import lombok.ToString;
import predicate.Constant;
import predicate.Expression;
import predicate.Predicate;
@ToString
public class UpdateData {
    @Getter private String tableName;
    @Getter private String fieldName;
    private Expression expression;
    @Getter private Predicate predicate;
    public UpdateData(String tableName,String fieldName,Expression expression,Predicate predicate){
        this.tableName=tableName;
        this.fieldName=fieldName;
        this.expression=expression;
        this.predicate=predicate;
    }

    public Constant<?> getValue(){
        return expression.getValue(null);
    }
}
