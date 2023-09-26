package jdbc;

import jdbc.adapter.ResultSetMetaDataAdapter;
import record.Schema;

import java.sql.SQLException;

public class MyResultSetMetaData extends ResultSetMetaDataAdapter {

    Schema schema;

    public MyResultSetMetaData(Schema schema){
        this.schema=schema;
    }

    @Override
    public int getColumnCount() throws SQLException {
        return schema.getFieldList().size();
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        return schema.getFieldList().get(column).getType();
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        return schema.getFieldList().get(column).getName();
    }
}
