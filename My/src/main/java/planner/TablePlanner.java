package planner;

import metadata.IndexInfo;
import metadata.MetadataManager;
import plan.*;
import predicate.Predicate;
import predicate.Term;
import transaction.Transaction;

import java.util.*;

public class TablePlanner {

    Predicate predicate;
    List<Term> joinTerms;

    Map<String, IndexInfo> indexes;

    Plan plan;

    int bufferAvailable;
    Transaction transaction;

    public TablePlanner(String tableName, Predicate predicate,List<Term>joinTerms, Transaction transaction,
                        MetadataManager metadataManager,int bufferAvailable){
        this.bufferAvailable=bufferAvailable;
        this.transaction=transaction;

        this.joinTerms=joinTerms;
        //predicate只可能是 this.field=const|this.field
        //join terms只可能是 other.field=this.field

        //找可能使用索引的谓词

        indexes=metadataManager.getTableIndexInfos(tableName);

        List<Term> indexableTerms=new ArrayList<>();
        for (Term term: predicate.getTerms()){
            if(term.getRightExpression().asFieldName()==null&&
                indexes.get(term.getLeftExpression().asFieldName())!=null){
                indexableTerms.add(term);
            }
        }

        TablePlan tablePlan=new TablePlan(transaction,metadataManager.getTableStatistics(tableName),
                metadataManager.getTableLayout(tableName),metadataManager.getTableSchema(tableName),tableName);

        if(indexableTerms.isEmpty()){
            this.plan=new SelectPlan(tablePlan,predicate);
            return;
        }

        //找到最佳索引
        indexableTerms.sort(Comparator.comparingInt(t -> t.reductionFactor(tablePlan)));
        Term bestTerm=indexableTerms.remove(indexableTerms.size()-1);

        //决定是否使用索引
        Predicate p1=new Predicate();
        p1.addTerm(indexableTerms.remove(indexableTerms.size()-1));
        SelectPlan select1=new SelectPlan(tablePlan,p1);
        IndexSelectPlan  index1=new IndexSelectPlan(tablePlan,transaction,bestTerm,indexes.get(bestTerm.getLeftExpression().asFieldName()));

        if(select1.getBlockAccessedNumber()<index1.getBlockAccessedNumber()){
            //不使用索引
            plan=new SelectPlan(tablePlan,predicate);
        }else{
            plan=index1;
            predicate.removeTerm(bestTerm);
            plan=new SelectPlan(plan,predicate);
        }
    }

    public Plan getPlan() {
        return plan;
    }

    public Plan tryJoin(Plan currentPlan){
        IndexJoinPlan index=tryIndexJoin(currentPlan);
        HashJoinPlan hash=tryHashJoin(currentPlan);
        MergeJoinPlan merge=tryMergeJoin(currentPlan);
        Plan winner=hash.cost()< merge.cost()?hash:merge;
        winner=index.getBlockAccessedNumber()+index.cost()<winner.cost()+ winner.getBlockAccessedNumber()?index:winner;
        return winner;
    }

    public IndexJoinPlan tryIndexJoin(Plan currentPlan){
        //判断能否使用index join
        for(String field:indexes.keySet()){
            for(Term term:joinTerms){
                //找到了能用索引的term
                if(term.getRightExpression().asFieldName().equals(field)){
                    if(currentPlan.getSchema().hasField(term.getLeftExpression().asFieldName())){
                        return new IndexJoinPlan(transaction,currentPlan,plan,term.getLeftExpression().asFieldName(),indexes.get(field));
                    }
                }
            }
        }
        return null;
    }

    public HashJoinPlan tryHashJoin(Plan currentPlan){
        for(Term term:joinTerms){
            if(currentPlan.getSchema().hasField(term.getLeftExpression().asFieldName())){
                return new HashJoinPlan(transaction,currentPlan,plan,term.getLeftExpression().asFieldName(),
                        term.getRightExpression().asFieldName(),bufferAvailable);
            }
        }
        return null;
    }

    public MergeJoinPlan tryMergeJoin(Plan currentPlan){
        for(Term term:joinTerms){
            if(currentPlan.getSchema().hasField(term.getLeftExpression().asFieldName())){
                return new MergeJoinPlan(transaction,currentPlan,plan,term.getLeftExpression().asFieldName(),
                        term.getRightExpression().asFieldName(),bufferAvailable);
            }
        }
        return null;
    }

    public Plan makeProduct(Plan currnetPlan){
        Plan plan1=new MultipleProductPlan(transaction,currnetPlan,plan, bufferAvailable);
        Plan plan2=new MultipleProductPlan(transaction,new MaterializePlan(currnetPlan,transaction),plan,bufferAvailable);
        return plan1.cost()+plan1.getBlockAccessedNumber()<plan2.cost()+plan2.getBlockAccessedNumber()?
                plan1:plan2;
    }
}
