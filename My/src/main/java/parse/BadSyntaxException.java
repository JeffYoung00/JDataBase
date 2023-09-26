package parse;

public class BadSyntaxException extends RuntimeException{
    public BadSyntaxException(String val){
        super(val);
    }
}
