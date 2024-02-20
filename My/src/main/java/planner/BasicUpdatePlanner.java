package planner;

import metadata.IndexInfo;
import metadata.MetadataManager;
import parse.data.*;
import plan.IndexSelectPlan;
import plan.Plan;
import plan.SelectPlan;
import plan.TablePlan;
import predicate.Predicate;
import predicate.Term;
import record.Layout;
import record.Schema;
import record.TableScan;
import scan.Scan;
import scan.UpdateScan;
import transaction.Transaction;

import java.util.*;

//todo 更新统计数据,更新index
public class BasicUpdatePlanner implements UpdatePlanner{

    MetadataManager metadataManager;

    private boolean empty=false;
    private Term bestTerm=null;

    private Map<String,IndexInfo> indexInfos;
    private Transaction transaction;

    public BasicUpdatePlanner(MetadataManager metadataManager,Transaction transaction){
        this.metadataManager=metadataManager;
        this.transaction=transaction;
    }

    @Override
    public int executeInsert(InsertData insertData) {
        indexInfos = metadataManager.getTableIndexInfos(insertData.getTableName());

        String tableName = insertData.getTableName();
        Layout layout = metadataManager.getTableLayout(tableName);
        TableScan tableScan=new TableScan(transaction,layout,tableName);
        tableScan.insert();
        for(int i=0;i<insertData.getFieldNameList().size();i++){
            tableScan.setValue(insertData.getFieldNameList().get(i),insertData.getValueList().get(i));
        }
        return 1;
    }

    @Override
    public int executeUpdate(UpdateData updateData) {
        indexInfos = metadataManager.getTableIndexInfos(updateData.getTableName());
        TablePlan tablePlan=new TablePlan(transaction,metadataManager.getTableStatistics(updateData.getTableName()),
                metadataManager.getTableLayout(updateData.getTableName()),
                metadataManager.getTableSchema(updateData.getTableName()),updateData.getTableName());
        classifyTerms(updateData.getTableName(), updateData.getPredicate().getTerms(),tablePlan);
        if(empty){
            return 0;
        }

        //决定是否使用index
        Plan bestPlan = null;
        if(bestTerm!=null){
            Plan plan1=new SelectPlan(new IndexSelectPlan(tablePlan,transaction,bestTerm,getIndexInfo(bestTerm)),updateData.getPredicate());
            Plan plan2=new SelectPlan(new SelectPlan(tablePlan,new Predicate(Collections.singletonList(bestTerm))),updateData.getPredicate());

            if(plan1.getBlockAccessedNumber()<plan2.getBlockAccessedNumber()){
                bestPlan=plan1;
            }else{
                bestPlan=plan2;
            }
        }else{
            bestPlan=new SelectPlan(tablePlan,updateData.getPredicate());
        }

        //执行
        UpdateScan open = (UpdateScan) bestPlan.open();
        int count=0;
        while(open.hasNext()){
            count++;
            open.setValue(updateData.getFieldName(),updateData.getValue() );
        }
        return count;
    }

    @Override
    public int executeDelete(DeleteData deleteData) {
        indexInfos = metadataManager.getTableIndexInfos(deleteData.getTableName());
        TablePlan tablePlan=new TablePlan(transaction,metadataManager.getTableStatistics(deleteData.getTableName()),
                metadataManager.getTableLayout(deleteData.getTableName()),
                metadataManager.getTableSchema(deleteData.getTableName()),deleteData.getTableName());
        classifyTerms(deleteData.getTableName(), deleteData.getPredicate().getTerms(),tablePlan);
        if(empty){
            return 0;
        }

        //决定是否使用index
        Plan bestPlan = null;
        if(bestTerm!=null){
            Plan plan1=new SelectPlan(new IndexSelectPlan(tablePlan,transaction,bestTerm,getIndexInfo(bestTerm)),deleteData.getPredicate());
            Plan plan2=new SelectPlan(new SelectPlan(tablePlan,new Predicate(Collections.singletonList(bestTerm))),deleteData.getPredicate());

            if(plan1.getBlockAccessedNumber()<plan2.getBlockAccessedNumber()){
                bestPlan=plan1;
            }else{
                bestPlan=plan2;
            }
        }else{
            bestPlan=new SelectPlan(tablePlan,deleteData.getPredicate());
        }

        //执行
        UpdateScan open = (UpdateScan) bestPlan.open();
        int count=0;
        while(open.hasNext()){
            open.delete();
            count++;
        }
        return count;
    }

    @Override
    public int executeCreateTable(CreateTableData createTableData) {
        metadataManager.createTable(transaction,createTableData);
        return 0;
    }

    @Override
    public int executeCreateIndex(CreateIndexData createIndexData) {
        metadataManager.createIndex(transaction,createIndexData);
        return 0;
    }

    public void classifyTerms(String tableName,List<Term> terms,TablePlan tablePlan){
        List<Term> constantTerms=new ArrayList<>(1);
        List<Term> indexTerms=new ArrayList<>(1);
        for(Term term:terms){
            if(term.constantToConstant()){
                constantTerms.add(term);
            }else if(term.fieldToConstant()&& metadataManager.hasIndex(tableName,term.getLeftExpression().asFieldName())){
                indexTerms.add(term);
            }
        }

        //移除constant to constant
        for(Term term:constantTerms){
            if(!term.isSatisfied(null)){
                empty=true;
                return;
            }
            terms.remove(term);
        }

        //找到能使用index的best term
        bestTerm=null;
        for(Term term:indexTerms){
            if(bestTerm==null||term.reductionFactor(tablePlan)>bestTerm.reductionFactor(tablePlan)){
                bestTerm=term;
            }
        }
        if(bestTerm!=null){
            terms.remove(bestTerm);
        }
    }

    private IndexInfo getIndexInfo(Term term){
        for(IndexInfo info:indexInfos.values()){
            if(term.getLeftExpression().asFieldName().equals(info.getField().getName())){
                return info;
            }
        }
        return null;
    }
}
