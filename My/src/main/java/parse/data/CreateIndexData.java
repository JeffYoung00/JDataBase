package parse.data;

import lombok.Getter;
import lombok.ToString;
import metadata.IndexManager;
import server.Database;

@ToString
@Getter
public class CreateIndexData {
    private String indexName;
    private String tableName;
    private String fieldName;
    private int type;
    public CreateIndexData(String indexName, String tableName, String fieldName){
        this.indexName=indexName;
        this.tableName=tableName;
        this.fieldName=fieldName;
        this.type= Database.BTree_Index;
    }

    public CreateIndexData(String indexName,String tableName,String fieldName,int type){
        this.indexName=indexName;
        this.tableName=tableName;
        this.fieldName=fieldName;
        if(type==1){
            this.type=Database.Hash_Index;
        }else{
            this.type= Database.BTree_Index;
        }
    }
}
