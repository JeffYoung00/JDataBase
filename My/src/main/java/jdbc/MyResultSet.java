package jdbc;

import jdbc.adapter.ResultSetAdapter;
import record.Schema;
import record.Field;
import plan.Plan;
import scan.Scan;

import java.sql.Connection;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

public class MyResultSet extends ResultSetAdapter {

    private Scan scan;
    private Schema schema;
    private Connection connection;
    private List<String> columnNames;

    public MyResultSet(Plan plan, Connection connection){
        this.scan=plan.open();
        this.schema=plan.getSchema();
        this.connection=connection;
        columnNames=schema.getFieldList().stream().map(Field::getName).collect(Collectors.toList());
    }

    @Override
    public boolean next() throws SQLException {
        try{
            return scan.hasNext();
        }catch(RuntimeException e){
            connection.rollback();
            throw new SQLException(e);
        }
    }

    @Override
    public void close() throws SQLException {
        scan.close();
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return new MyResultSetMetaData(schema);
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        return scan.getValue(columnNames.get(columnIndex)).getValue();
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        if(!columnNames.contains(columnLabel)){
            throw new SQLException("wrong field name");
        }
        return scan.getValue(columnLabel).getValue();
    }


//    @Override
//    public String getString(String columnLabel) throws SQLException {
//        return (String)scan.getValue(columnLabel).getValue();
//    }
//
//    @Override
//    public int getInt(String columnLabel) throws SQLException {
//        return (int)scan.getValue(columnLabel).getValue();
//    }
//
//    @Override
//    public String getString(int columnIndex) throws SQLException {
//        return getString(columnNames.get(columnIndex));
//    }
//
//    @Override
//    public int getInt(int columnIndex) throws SQLException {
//        return getInt(columnNames.get(columnIndex));
//    }
}