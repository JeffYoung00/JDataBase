package metadata;

import lombok.Getter;
import lombok.Setter;
import record.Field;

public class IndexInfo {
    @Getter String indexName;
    @Getter Field field;
    @Getter int indexType;
    @Setter
    @Getter int indexHeight;
    @Setter
    @Getter int rootBlockNumber;

    public IndexInfo(String indexName,Field field,int indexType,int indexHeight,int rootBlockNumber){
        this.indexName=indexName;
        this.field=field;
        this.indexType=indexType;
        this.indexHeight=indexHeight;
        this.rootBlockNumber=rootBlockNumber;
    }

    public int getGlobalDepth(){
        return -1;
    }

}
