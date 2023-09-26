package planner;

import parse.data.*;
import record.Schema;
import transaction.Transaction;

public interface UpdatePlanner {
    int executeInsert(InsertData insertData);
    int executeUpdate(UpdateData updateData);
    int executeDelete(DeleteData deleteData);
    int executeCreateTable(CreateTableData createTableData);
    int executeCreateIndex(CreateIndexData createIndexData);
}
