package parse;

import metadata.MetadataManager;
import metadata.TableManager;
import parse.data.*;
import predicate.Expression;
import predicate.Predicate;
import predicate.Term;
import record.Layout;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

//todo join的字段类型判断
//检查是否有字段语义错误
public class Semantics {

    private MetadataManager tableManager;

    public Semantics(MetadataManager tableManager){
        this.tableManager=tableManager;
    }

    public void checkSelect(SelectData selectData){
        Map<String, Layout> tableLayoutMap=new HashMap<>(selectData.getTableNames().size());

        for(String s: selectData.getTableNames()){
            Layout layout=tableManager.getTableLayout(s);
            if(layout==null){
                throw new BadSemanticsException(s,BadSemanticsException.Table_Not_Exist);
            }
            tableLayoutMap.put(s,layout);
        }

        List<Term> terms=selectData.getPredicate().getTerms();
        for(Term term:terms){
            if(!term.getLeftExpression().isConstantValue()){
                checkTableNameAndAdjust(term.getLeftExpression(),tableLayoutMap);
            }
            if(!term.getRightExpression().isConstantValue()){
                checkTableNameAndAdjust(term.getRightExpression(),tableLayoutMap);
            }
        }
    }

    //给select语句用,会在expression中补上table name
    private void checkTableNameAndAdjust(Expression expression, Map<String, Layout> layoutMap){
        String fieldName = expression.asFieldName();
        String tableName = expression.asTableName();

        //如果是a.b形式
        if(tableName!=null){
            Layout layout = layoutMap.get(tableName);
            if(layout==null||!layout.hasField(fieldName)){
                throw new BadSemanticsException(fieldName,BadSemanticsException.Field_Ambiguity);
            }
        }

        //如果是b形式,变成a.b形式
        int t=0;
        String newTableName=null;
        for(Map.Entry<String,Layout>entry:layoutMap.entrySet()){
            if(entry.getValue().hasField(fieldName)){
                newTableName=entry.getKey();
                t++;
            }
        }
        if(t==0){
            throw new BadSemanticsException(fieldName,BadSemanticsException.Table_Not_Exist);
        }else if(t>1){
            throw new BadSemanticsException(fieldName,BadSemanticsException.Field_Ambiguity);
        }
        expression.addTableName(newTableName);
    }

    //true,检查table是否存在
    //false,检查table是否不存在
    private void checkTableName(String tableName,boolean exist){
        if(tableManager.hasTable(tableName)!=exist){
            if(exist){
                throw new BadSemanticsException(tableName,BadSemanticsException.Table_Not_Exist);
            }else{
                throw new BadSemanticsException(tableName,BadSemanticsException.Table_Exist);
            }
        }
    }

    private void checkPredicateFieldNameInTable(String tableName, Predicate predicate){
        for(Term term:predicate.getTerms()){
            if(!term.getLeftExpression().isConstantValue()){
                checkFieldName(tableName, term.getLeftExpression().asFieldName(),true);
            }
            if(!term.getRightExpression().isConstantValue()){
                checkFieldName(tableName, term.getRightExpression().asFieldName(),true);
            }
        }
    }

    private void checkFieldName(String tableName,String fieldName,boolean exist){
        if(tableManager.hasField(tableName,fieldName)!=exist){
            if(exist){
                throw new BadSemanticsException(fieldName,BadSemanticsException.Field_Not_Exist);
            }else{
                throw new BadSemanticsException(fieldName,BadSemanticsException.Field_Exist);
            }
        }
    }

    public void checkInsert(InsertData insertData){
        checkTableName(insertData.getTableName(),true);
        for(String s:insertData.getFieldNameList()){
            checkFieldName(insertData.getTableName(),s,true);
        }
    }

    public void checkUpdate(UpdateData updateData){
        checkTableName(updateData.getTableName(), true);
        checkFieldName(updateData.getTableName(), updateData.getFieldName(), true);
        checkPredicateFieldNameInTable(updateData.getTableName(), updateData.getPredicate());
    }

    public void checkDelete(DeleteData deleteData){
        checkTableName(deleteData.getTableName(), false);
        checkPredicateFieldNameInTable(deleteData.getTableName(), deleteData.getPredicate());
    }

    public void checkCreateTable(CreateTableData createTableData){
        checkTableName(createTableData.getTableName(), false);
    }

    public void checkCreateIndex(CreateIndexData createIndexData){
        checkFieldName(createIndexData.getTableName(), createIndexData.getFieldName(), true);
        if(tableManager.hasIndex(createIndexData.getIndexName(),createIndexData.getFieldName())){
            throw new BadSemanticsException(createIndexData.getFieldName(), BadSemanticsException.Index_Exist);
        }
        if(tableManager.hasIndex(createIndexData.getIndexName())) {
            throw new BadSemanticsException(createIndexData.getIndexName(), BadSemanticsException.Index_Exist);
        }
    }
}
