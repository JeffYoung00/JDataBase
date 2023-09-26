package jdbc;

import jdbc.adapter.ConnectionAdapter;
import server.Database;
import transaction.Transaction;

import java.sql.SQLException;
import java.sql.Statement;

public class MyConnection extends ConnectionAdapter {
    Database database;
    private Transaction currentTransaction;

    public MyConnection(Database database){
        this.database=database;
        this.currentTransaction=database.newTransaction();
    }

    @Override
    public Statement createStatement() throws SQLException {
        return new MyStatement(this,database.planner());
    }

    @Override
    public void close() throws SQLException {
        currentTransaction.commit();
    }

    @Override
    public void commit() throws SQLException {
        currentTransaction.commit();
        currentTransaction=database.newTransaction();
    }

    @Override
    public void rollback() throws SQLException {
        currentTransaction.rollback();
        currentTransaction=database.newTransaction();
    }

    Transaction getCurrentTransaction(){
        return currentTransaction;
    }

//    @Override
//    public void setAutoCommit(boolean autoCommit) throws SQLException {
//        super.setAutoCommit(autoCommit);
//    }
//
//    @Override
//    public void setTransactionIsolation(int level) throws SQLException {
//
//    }
}
