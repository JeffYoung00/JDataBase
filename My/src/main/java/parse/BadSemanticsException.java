package parse;

public class BadSemanticsException extends RuntimeException{

    public static int Table_Not_Exist=0,Field_Not_Exist=1,Field_Ambiguity=2,Table_Exist=3,Field_Exist=4,Index_Exist=5;
    public static String [] cause={"table not exist: ","field not exist: ","field ambiguity : "};

    public BadSemanticsException(String s, int type){
        super(cause[type]+s);
    }
}
