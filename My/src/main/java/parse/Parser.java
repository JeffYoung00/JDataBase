package parse;

import java.util.ArrayList;
import java.util.List;

import parse.data.*;
import predicate.Constant;
import predicate.Expression;
import predicate.Predicate;
import predicate.Term;
import record.Field;
import record.Schema;


//出现了if就不要忘记eat
//else 就抛异常

public class Parser {
    private Lexer lexer;

    public Parser(String s) {
        lexer = new Lexer(s);
    }

    public Object parse(){
        Object ret=doParse();
        lexer.eatDelimiter(';');
        if(!lexer.isEnding()){
            throw new BadSyntaxException("expected ending after ';'");
        }
        return ret;
    }

    private Object doParse(){
        if(lexer.matchKeyWord("select")){
            return selectData();
        }else if(lexer.matchKeyWord("insert")){
            return insertData();
        }else if(lexer.matchKeyWord("update")){
            return updateData();
        }else if(lexer.matchKeyWord("delete")){
            return deleteDate();
        }else if(lexer.matchKeyWord("create")){
            lexer.eatKeyWord("create");
            if(lexer.matchKeyWord("table")){
                return createTableData();
            }else if(lexer.matchKeyWord("index")){
                return createIndexData();
            }else if(lexer.matchKeyWord("view")){
                return createViewData();
            }else{
                throw new BadSyntaxException("wrong creation type");
            }
        }else{
            throw new BadSyntaxException("wrong operation type");
        }
    }




    //predicate

    public Constant<?> constant(){
        if(lexer.matchStringConstant()){
            return new Constant<>(lexer.eatStringConstant());
        }else if(lexer.matchIntConstant()){
            return new Constant<>(lexer.eatIntConstant());
        }else {
            throw new BadSyntaxException("not right type constant");
        }
    }

    public Expression expression(){
        if(lexer.matchId()){
            String first=lexer.eatId();
            if(lexer.matchDelimiter('.')){
                lexer.eatDelimiter('.');
                return new Expression(first,lexer.eatId());
            }else{
                return new Expression(first);
            }
        }else{
            return new Expression(constant());
        }
    }

    public Term term(){
        Expression leftExpression=expression();
        lexer.eatDelimiter('=');
        Expression rightExpression=expression();
        return new Term(leftExpression,rightExpression);
    }

    public Predicate predicate() {
        Term term=term();
        List<Term> terms=new ArrayList<>();
        terms.add(term);
        while(lexer.matchKeyWord("and")){
            lexer.eatKeyWord("and");
            terms.add(term());
        }
        return new Predicate(terms);
    }

// Methods for parsing queries

    public List<String> idList(){
        String id = lexer.eatId();
        List<String> ret=new ArrayList<>();
        ret.add(id);
        while(lexer.matchDelimiter(',')){
            lexer.eatDelimiter(',');
            ret.add(lexer.eatId());
        }
        return ret;
    }

    public List<Constant<?>> constantList(){
        Constant<?> id = constant();
        List<Constant<?>> ret=new ArrayList<>();
        ret.add(id);
        while(lexer.matchDelimiter(',')){
            lexer.eatDelimiter(',');
            ret.add(constant());
        }
        return ret;
    }

    public SelectData selectData(){
        lexer.eatKeyWord("select");
        List<String> fieldNameList=idList();
        lexer.eatKeyWord("from");
        List<String> tableNameList=idList();
        Predicate predicate=new Predicate();
        if(lexer.matchKeyWord("where")){
            lexer.eatKeyWord("where");
            predicate=predicate();
        }
        return new SelectData(fieldNameList,tableNameList,predicate);
    }

    public InsertData insertData(){
        lexer.eatKeyWord("insert");
        lexer.eatKeyWord("into");
        String tableName=lexer.eatId();
        lexer.eatDelimiter('(');
        List<String> fields=idList();
        lexer.eatDelimiter(')');
        lexer.eatKeyWord("values");
        lexer.eatDelimiter('(');
        List<Constant<?>> constants=constantList();
        lexer.eatDelimiter(')');
        return new InsertData(tableName,fields,constants);
    }

    public UpdateData updateData(){
        lexer.eatKeyWord("update");
        String tableName= lexer.eatId();
        lexer.eatKeyWord("set");
        String fieldName=lexer.eatId();
        lexer.eatDelimiter('=');
        Expression  expression=expression();
        Predicate predicate=new Predicate();
        if(lexer.matchKeyWord("where")){
            lexer.eatKeyWord("where");
            predicate=predicate();
        }
        return new UpdateData(tableName,fieldName,expression,predicate);
    }

    public DeleteData deleteDate(){
        lexer.eatKeyWord("delete");
        lexer.eatKeyWord("from");
        String tableName=lexer.eatId();
        Predicate predicate=new Predicate();
        if(lexer.matchKeyWord("where")){
            lexer.eatKeyWord("where");
            predicate=predicate();
        }
        return new DeleteData(tableName,predicate);
    }



// Method for parsing create table commands

    public Field field(){
        String name=lexer.eatId();
        if(lexer.matchKeyWord("int")){
            lexer.eatKeyWord("int");
            return new Field(name,Field.Integer);
        }else if(lexer.matchKeyWord("varchar")){
            lexer.eatKeyWord("varchar");
            lexer.eatDelimiter('(');
            int len=lexer.eatIntConstant();
            lexer.eatDelimiter(')');
            return new Field(name,Field.String,len);
        }else{
            throw new BadSyntaxException("wrong type");
        }
    }

    public List<Field> fields(){
        List<Field> ret=new ArrayList<>();
        ret.add(field());
        while(lexer.matchDelimiter(',')){
            lexer.eatDelimiter(',');
            ret.add(field());
        }
        return ret;
    }

    public Schema createTableData(){
        lexer.eatKeyWord("table");
        String tableName=lexer.eatId();
        lexer.eatDelimiter('(');
        List<Field> fields=fields();
        lexer.eatDelimiter(')');
        return new Schema(fields);
    }

    public CreateViewData createViewData() {
        lexer.eatKeyWord("view");
        String viewName = lexer.eatId();
        lexer.eatKeyWord("as");
        return new CreateViewData(viewName,selectData());
    }

    public CreateIndexData createIndexData() {
        lexer.eatKeyWord("index");
        String indexName = lexer.eatId();
        lexer.eatKeyWord("on");
        String tableName = lexer.eatId();
        lexer.eatDelimiter('(');
        String fieldName = lexer.eatId();
        lexer.eatDelimiter(')');
        return new CreateIndexData(indexName,tableName,fieldName);
    }
}
