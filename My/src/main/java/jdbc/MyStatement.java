package jdbc;

import jdbc.adapter.StatementAdapter;
import parse.Parser;
import parse.data.SelectData;
import plan.Plan;
import planner.Planner;
import server.Database;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

public class MyStatement extends StatementAdapter {

    Planner planner;
    MyConnection connection;

    ResultSet resultSet;
    int updateCount;

    public MyStatement(MyConnection connection, Planner planner){
        this.planner=planner;
        this.connection=connection;
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        try{
            //关闭上一轮资源
            close();

            Parser parser=new Parser(sql);
            Object data=parser.parse();
            if(data instanceof SelectData){
                Plan queryPlan=planner.createPlan((SelectData)data,connection.getCurrentTransaction());
                resultSet=new MyResultSet(queryPlan,connection);
                return true;
            }else{
                updateCount=planner.executeUpdate(data,connection.getCurrentTransaction());
                return false;
            }

            //if connection.autoCommit
        }catch (RuntimeException e){
            connection.rollback();
            throw new SQLException(e);
        }
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        return resultSet;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        return updateCount;
    }

    @Override
    public void close() throws SQLException {
        if(resultSet!=null){
            resultSet.close();
        }
        resultSet=null;
        updateCount=-1;
    }

//    @Override
//    public ResultSet executeQuery(String sql) throws SQLException {
//        return super.executeQuery(sql);
//    }
//
//    @Override
//    public int executeUpdate(String sql) throws SQLException {
//        return super.executeUpdate(sql);
//    }
//

}
