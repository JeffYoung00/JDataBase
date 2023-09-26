package planner;

import cache.CacheManager;
import metadata.MetadataManager;
import parse.Semantics;
import parse.data.*;
import plan.Plan;
import record.Schema;
import transaction.Transaction;

public class Planner {
    private Semantics semantics;
    private MetadataManager metadataManager;
    private CacheManager cacheManager;

    public Planner(MetadataManager metadataManager, CacheManager cacheManager, Semantics semantics){
        this.metadataManager=metadataManager;
        this.semantics=semantics;
        this.cacheManager=cacheManager;
    }

    public Plan createPlan(SelectData selectData,Transaction transaction){

//        QueryPlanner queryPlanner=new HeuristicQueryPlanner(metadataManager,Math.min(cacheManager.getFreeCaches()/10,3),transaction);
        QueryPlanner queryPlanner=new BasicQueryPlanner(metadataManager,Math.min(cacheManager.getFreeCaches()/10,3),transaction);

        semantics.checkSelect(selectData);
        return queryPlanner.createPlan(selectData);
    }

    public int executeUpdate(Object data,Transaction transaction){
        UpdatePlanner updatePlanner=new BasicUpdatePlanner(metadataManager,transaction);
        if(data instanceof InsertData){
            semantics.checkInsert((InsertData) data);
            return updatePlanner.executeInsert((InsertData) data);
        }else if(data instanceof UpdateData){
            semantics.checkUpdate((UpdateData) data);
            return updatePlanner.executeUpdate((UpdateData) data);
        }else if(data instanceof DeleteData){
            semantics.checkDelete((DeleteData) data);
            return updatePlanner.executeDelete((DeleteData) data);
        }else if(data instanceof CreateTableData){
            semantics.checkCreateTable((CreateTableData) data);
            return updatePlanner.executeCreateTable((CreateTableData) data);
        }else if(data instanceof CreateIndexData){
            semantics.checkCreateIndex((CreateIndexData) data);
            return updatePlanner.executeCreateIndex((CreateIndexData) data);
        }else{
            return -1;
        }
    }

}
