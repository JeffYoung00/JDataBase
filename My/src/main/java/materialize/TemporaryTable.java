package materialize;

import lombok.Getter;
import record.Layout;
import record.TableScan;
import server.Database;
import transaction.Transaction;

public class TemporaryTable {
    public static int TempNumber=-1;
    public static synchronized String getNextTempTableName(){
        TempNumber++;
        return "Temp"+TempNumber+ Database.tempTablePostfix;
    }

    private Transaction transaction;
    @Getter private Layout layout;
    @Getter private String fileName;


    public TemporaryTable(Transaction transaction, Layout layout) {
        this.transaction = transaction;
        this.layout=layout;
        this.fileName=getNextTempTableName();
    }

    /**
     * Open a table scan for the temporary table.
     */
    public TableScan open() {
        return new TableScan(transaction, layout,fileName);
    }
}
