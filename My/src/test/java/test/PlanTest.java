package test;

import metadata.MetadataManager;
import plan.TablePlan;
import predicate.Constant;
import record.TableScan;
import scan.Scan;
import server.Database;
import transaction.Transaction;

public class PlanTest {
    public static void main(String[] args) {
        Database database=new Database("testDB");
        Transaction transaction=database.newTransaction();
        MetadataManager metadataManager=database.metadataManager();
        TablePlan tablePlan=new TablePlan(transaction,metadataManager.getTableStatistics("testtable0"),
                metadataManager.getTableLayout("testtable0"),metadataManager.getTableSchema("testtable0"),
                "testtable0");
        Scan tableScan= tablePlan.open();

//        TableScan tableScan=new TableScan(transaction,metadataManager.getTableLayout("testtable0"),"testtable0");
        while(tableScan.hasNext()){
            String  name = (String) tableScan.getValue("name").getValue();
            System.out.println(name);
        }
        tableScan.close();
    }
}
