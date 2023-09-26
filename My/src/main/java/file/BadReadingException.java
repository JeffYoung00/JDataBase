package file;

/**
 * file manager reading 超出范围
 */
public class BadReadingException extends RuntimeException{
    public BadReadingException(String message){
        super(message);
    }
}
