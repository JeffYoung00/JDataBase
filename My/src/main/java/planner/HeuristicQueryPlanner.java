package planner;

import metadata.MetadataManager;
import parse.data.SelectData;
import plan.Plan;
import plan.ProjectPlan;
import plan.SelectPlan;
import plan.TablePlan;
import predicate.Predicate;
import predicate.Term;
import transaction.Transaction;

import java.util.*;

public class HeuristicQueryPlanner implements QueryPlanner{
    private MetadataManager metadataManager;

    //所有的表对应的plan
    private List<TablePlanner> tablePlanners;
    private List<Integer> maxConnectedTables;
    private boolean empty=false;

    private int bufferAvailable;
    private Transaction transaction;

    public HeuristicQueryPlanner(MetadataManager metadataManager, int bufferAvailable, Transaction transaction){
        this.metadataManager=metadataManager;
        this.bufferAvailable=bufferAvailable;
        this.transaction=transaction;
    }

    public Plan createPlan(SelectData selectData){

        classifyTerms(selectData);

        //得到第一个表,如果
        Plan currentPlan;
        if(!maxConnectedTables.isEmpty()){
            currentPlan = getLeastRecordsPlan(maxConnectedTables);
        }else{
            currentPlan = getLeastRecordsPlan();
        }
        if(empty){
            currentPlan=new SelectPlan(currentPlan,Predicate.emptyPredicate);
        }

        //不断join
        while(!tablePlanners.isEmpty()){
            Plan plan=getLowestJoinPlan(currentPlan);
            //没有可以join的table
            if(plan!=null){
                currentPlan=plan;
            }else{
                currentPlan=getLowestProductPlan(currentPlan);
            }
        }

        return new ProjectPlan(currentPlan,selectData.getResultFields());
    }

    //分析terms,并得到table planners和max connections
    public void classifyTerms(SelectData selectData){
        List<List<Integer>> tableRelations=new ArrayList<>();
        List<Term> terms=selectData.getPredicate().getTerms();
        List<Term> constantTerms=new ArrayList<>(1);
        List<Predicate> predicates=new ArrayList<>(selectData.getTableNames().size());
        List<List<Term>> joinTerms=new ArrayList<>();

        for(int i=0;i<selectData.getTableNames().size();i++){
            predicates.add(new Predicate());
        }
        for(String s:selectData.getTableNames()){
            tableRelations.add(new ArrayList<>(selectData.getTableNames().size()/2));
            joinTerms.add(new ArrayList<>(selectData.getTableNames().size()/2));
        }
        for(Term term:terms){
            String leftTable=term.getLeftExpression().asTableName();
            String rightTable=term.getRightExpression().asTableName();
            //找到用于两表关联的term
            // a.f1=b.f2 加入b
            // b.f2=a.f1 加入a
            if( leftTable!=null && rightTable!=null&& !leftTable.equals(rightTable)) {
                int leftIndex=selectData.getTableNames().indexOf(leftTable);
                int rightIndex=selectData.getTableNames().indexOf(rightTable);
                tableRelations.get(leftIndex).add(rightIndex);
                joinTerms.get(leftIndex).add(term.reverseTerm());

                tableRelations.get(rightIndex).add(leftIndex);
                joinTerms.get(rightIndex).add(term);

            }//适用于一个表的term
            else if(leftTable!=null){
                int tableIndex=selectData.getTableNames().indexOf(leftTable);
                predicates.get(tableIndex).addTerm(term);
            }//常量term
            else{
                constantTerms.add(term);
            }
        }

        //取出常量的关系
        if(!constantTerms.isEmpty()){
            for(Term term:constantTerms){
                if(!term.isSatisfied(null)){
                    empty=true;
                }
            }
        }

        //将每个predicate交给对应的table planner
        tablePlanners=new ArrayList<>(selectData.getTableNames().size());
        for(int i=0;i<selectData.getTableNames().size();i++){
            TablePlanner tablePlanner=new TablePlanner(selectData.getTableNames().get(i),predicates.get(i),
                    joinTerms.get(i),transaction,metadataManager,bufferAvailable);
            tablePlanners.add(tablePlanner);
        }

        //找一个最大的连通分量,第一个左表需要在其中
        maxConnectedTables=Utils.maxConnectedComponents(tableRelations);
    }

    private Plan getLeastRecordsPlan(List<Integer>maxConnectedTables){
        int minIndex=maxConnectedTables.get(0);
        int minRecords=Integer.MAX_VALUE;
        for(Integer i:maxConnectedTables){
            if(tablePlanners.get(i).getPlan().getRecordNumber()<minRecords){
                minIndex=i;
                minRecords=tablePlanners.get(i).getPlan().getRecordNumber();
            }
        }
        Plan ret=tablePlanners.get(minIndex).getPlan();
        tablePlanners.remove(minIndex);
        return ret;
    }

    private Plan  getLeastRecordsPlan(){
        int minIndex=0;
        int minRecords=Integer.MAX_VALUE;
        for(int i=0;i<tablePlanners.size();i++){
            if(tablePlanners.get(i).getPlan().getRecordNumber()<minRecords){
                minIndex=i;
                minRecords=tablePlanners.get(i).getPlan().getRecordNumber();
            }
        }
        Plan ret=tablePlanners.get(minIndex).getPlan();
        tablePlanners.remove(minIndex);
        return ret;
    }

    private Plan getLowestJoinPlan(Plan currentPlan){
        Plan bestPlan=null;
        for(TablePlanner tablePlanner:tablePlanners){
            Plan plan=tablePlanner.tryJoin(currentPlan);
            if( plan!=null&&(bestPlan==null||bestPlan.getRecordNumber()<plan.getRecordNumber())){
                bestPlan=plan;
            }
        }
        return bestPlan;
    }

    private Plan getLowestProductPlan(Plan currentPlan){
        Plan bestPlan=null;
        for(TablePlanner tablePlanner:tablePlanners){
            Plan plan=tablePlanner.makeProduct(currentPlan);
            if(bestPlan==null||bestPlan.getRecordNumber()<plan.getRecordNumber()){
                bestPlan=plan;
            }
        }
        return bestPlan;
    }
}
