package planner;

import metadata.MetadataManager;
import parse.data.SelectData;
import plan.*;
import record.TableScan;
import transaction.Transaction;

import java.util.ArrayList;
import java.util.List;

/**
 * tableScan->productScan->select->project
 */
public class BasicQueryPlanner implements QueryPlanner{

    private MetadataManager metadataManager;
    private int bufferAvailable;
    private Transaction transaction;

    public BasicQueryPlanner(MetadataManager metadataManager, int bufferAvailable, Transaction transaction){
        this.metadataManager=metadataManager;
        this.bufferAvailable=bufferAvailable;
        this.transaction=transaction;
    }

    public Plan createPlan(SelectData selectData){
        List<TablePlan> plans=new ArrayList<>();
        for(String s:selectData.getTableNames()){
            TablePlan tableScan = new TablePlan(transaction,metadataManager.getTableStatistics(s),
                    metadataManager.getTableLayout(s), metadataManager.getTableSchema(s),s);
            plans.add( tableScan );
        }
        Plan ret=plans.remove(0);
        while(!plans.isEmpty()){
            ret=new ProductPlan(ret,plans.remove(0));
        }
        ret=new SelectPlan(ret,selectData.getPredicate());
        ret=new ProjectPlan(ret,selectData.getResultFields());
        return ret;
    }
}
