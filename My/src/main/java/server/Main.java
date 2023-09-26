package server;

import jdbc.MyDriver;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

public class Main {
    static String databaseName="testDB";

    public static void main(String[] args) throws SQLException{
        startLocalServer();
        databaseFront();
    }

    static Database database;
    //注册失败就终止
    public static void startLocalServer()throws SQLException{
        //启动所有database

        database = new Database(databaseName);
        //加入map
        Map<String,Database> databaseMap=new HashMap<>();
        databaseMap.put(databaseName,database);

        MyDriver myDriver=new MyDriver(databaseMap);
        DriverManager.registerDriver(myDriver);
    }

    public static void closeLocalServer(){
        database.normalShutdown();
    }

    public static void databaseFront(){
        Scanner scanner=new Scanner(System.in);
        try {
            Connection connection= DriverManager.getConnection(Database.urlPrefix+databaseName);
            Statement statement=connection.createStatement();

            boolean commit=true;
            while(true){
                try{
                    String sql = scanner.nextLine().trim().toLowerCase();
                    if(sql.equals("begin;")){
                        commit=false;
                    }else if(sql.equals("commit;")){
                        commit=true;
                    }else if(sql.equals("rollback;")){
                        connection.rollback();
                        commit=true;
                        continue;
                    }else if(sql.equals("exit;")){
                        break;
                    }else if(statement.execute(sql)){
                        ResultSet resultSet= statement.getResultSet();
                        showResult(resultSet);
                    }else{
                        int updateCount=statement.getUpdateCount();
                        System.out.println("update lines count: "+updateCount);
                    }
                    if(commit){
                        connection.commit();
                    }
                }catch(SQLException e){
                    connection.rollback();
                    e.printStackTrace();
                }
            }
            statement.close();
            connection.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            closeLocalServer();
        }
    }


    public static void showResult(ResultSet resultSet) throws SQLException {
        ResultSetMetaData metaData = resultSet.getMetaData();
        for(int i=0;i<metaData.getColumnCount();i++){
            System.out.print(" field: ");
            System.out.print(metaData.getColumnName(i));
            System.out.print(" type:");
            System.out.println(metaData.getColumnType(i));
        }

        while(resultSet.next()){
            for(int i=0;i<metaData.getColumnCount();i++){
                System.out.print(resultSet.getObject(i));
                System.out.print("  ");
            }
            System.out.println();
        }
    }
}
