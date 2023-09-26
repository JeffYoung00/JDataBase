package predicate;

import lombok.Getter;
import lombok.ToString;
import plan.Plan;
import scan.Scan;

import java.util.Objects;

@ToString
public class Term {

    //对于某些意义不明的term,假定筛选剩下一半
    private static int Uncountable_Factor=2;


    @Getter private Expression leftExpression,rightExpression;
    public Term(Expression leftExpression,Expression rightExpression){
        this.leftExpression=leftExpression;
        this.rightExpression=rightExpression;
        if(leftExpression.isConstantValue()&&!rightExpression.isConstantValue()){
            this.leftExpression=rightExpression;
            this.rightExpression=leftExpression;
        }
    }

    public boolean isSatisfied(Scan scan){
        Constant<?> leftValue=leftExpression.getValue(scan);
        Constant<?> rightValue=rightExpression.getValue(scan);
        return leftValue.equals(rightValue);
    }

    //field = const
    //field = field
    //const = const
    public int reductionFactor(Plan plan){
        if(!leftExpression.isConstantValue()&& !rightExpression.isConstantValue()){
            //用于join的field=field已被剔除
            //剩下这种情况是意义不明
            return Uncountable_Factor;
        }else if(!leftExpression.isConstantValue() ){
            return plan.getFieldDistinctValues(leftExpression.asFieldName());
        }else{
            //这种情况会被一开始剔除
            if(Objects.equals(leftExpression.asConstantValue(),rightExpression.asConstantValue())){
                return 1;
            }else{
                return Integer.MAX_VALUE;
            }
        }
    }

    public Term reverseTerm(){
        return new Term(rightExpression,leftExpression);
    }

    public boolean fieldToConstant(){
        return !leftExpression.isConstantValue()&& rightExpression.isConstantValue();
    }

    public boolean constantToConstant(){
        return leftExpression.isConstantValue()&& rightExpression.isConstantValue();
    }

}
