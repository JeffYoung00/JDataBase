package server;

import server.Database;

public class DatabaseException extends RuntimeException{
    public DatabaseException(String cause){
        super(cause);
    }
}
