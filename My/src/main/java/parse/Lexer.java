package parse;

import java.io.IOException;
import java.io.StreamTokenizer;
import java.io.StringReader;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class Lexer {

    StreamTokenizer tokenizer;

    private List<String> keyWords = Arrays.asList(
            "select", "from", "where", "insert", "into", "values", "delete", "update", "set","join","on",
            "create", "table","index","view",
            "and","or","is","null","as",
            "int", "varchar");

    public Lexer(String s){
        tokenizer=new StreamTokenizer(new StringReader(s));
        //添加 _ 作为 word char
        tokenizer.wordChars('_','_');
        //让 . 作为 普通分隔符
        tokenizer.ordinaryChar('.');
        //自动小写
        tokenizer.lowerCaseMode(true);
        nextToken();
    }

    public void nextToken(){
        try{
            tokenizer.nextToken();
        }catch (IOException e) {
            throw new BadSyntaxException("io err next");
        }
    }

    public boolean isEnding(){
        return tokenizer.ttype==StreamTokenizer.TT_EOF;
    }

    public boolean matchStringConstant(){
        return tokenizer.ttype=='\''||tokenizer.ttype=='\"';
    }

    public boolean matchIntConstant(){
        return tokenizer.ttype==StreamTokenizer.TT_NUMBER;
    }

    public boolean matchKeyWord(String keyWord){
        return tokenizer.ttype==StreamTokenizer.TT_WORD && Objects.equals(tokenizer.sval, keyWord);
    }

    public boolean matchId(){
        return tokenizer.ttype==StreamTokenizer.TT_WORD && !keyWords.contains(tokenizer.sval);
    }

    public boolean matchDelimiter(char delimiter){
        return tokenizer.ttype==delimiter;
    }

    public String eatStringConstant(){
        if(matchStringConstant()){
            String ret= tokenizer.sval;
            nextToken();
            return ret;
        }else{
            throw new BadSyntaxException("not string constant");
        }
    }

    public int eatIntConstant(){
        if(matchIntConstant()){
            int ret=(int)tokenizer.nval;
            nextToken();
            return ret;
        }else{
            throw new BadSyntaxException("not int constant");
        }
    }

    public String eatKeyWord(String keyWord){
        if(matchKeyWord(keyWord)){
            String ret= tokenizer.sval;
            nextToken();
            return ret;
        }else{
            throw new BadSyntaxException("not int constant");
        }
    }

    public String eatId(){
        if(matchId()){
            String ret= tokenizer.sval;
            nextToken();
            return ret;
        }else{
            throw new BadSyntaxException("not right id: "+tokenizer.sval);
        }
    }

    public char eatDelimiter(char delimiter){
        if(matchDelimiter(delimiter)){
            char ret= (char) tokenizer.ttype;
            nextToken();
            return ret;
        }else{
            throw new BadSyntaxException("not right delimiter: "+delimiter);
        }
    }
}
