package jdbc;

import jdbc.adapter.DriverAdapter;
import server.Database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

public class MyDriver extends DriverAdapter {

    private Map<String,Database> databaseMap;

    public MyDriver(Map<String, Database> databaseMap){
        this.databaseMap=databaseMap;
    }

//    static {
//        try {
//            DriverManager.registerDriver(new MyDriver());
//        } catch (SQLException e) {
//            e.printStackTrace();
//        }
//    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        String databaseName=url.replace(Database.urlPrefix,"");
        Database database=databaseMap.get(databaseName);
        if(database==null){
            throw new SQLException("no such database");
        }
        return new MyConnection(database);
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        if(url.startsWith("jhy://")){
            return true;
        }
        return false;
    }
}
