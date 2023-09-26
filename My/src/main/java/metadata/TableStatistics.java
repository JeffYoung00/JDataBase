package metadata;


import java.util.Map;

public class TableStatistics {

    int blockNumber;
    int recordNumber;

    //field name to distinct rate
    private Map<String,Double> distinctRateMap;

    int initRecordNumber;
    int modifyCount;


    public TableStatistics(int recordNumber,int blockNumber,Map<String,Double>distinctRateMap){
        this.recordNumber=this.initRecordNumber=recordNumber;
        this.blockNumber=blockNumber;
        this.distinctRateMap=distinctRateMap;
        this.modifyCount=0;
    }

    public int getBlockAccessedNumber(){
        return blockNumber*recordNumber/initRecordNumber;
    }

    public int getRecordNumber(){
        return recordNumber;
    }

    public int getFieldDistinctValues(String fieldName){
        return (int)(distinctRateMap.get(fieldName)*recordNumber);
    }

    public void modify(int count){
        modifyCount+=count;
    }

    public boolean decideReplace(){
        return modifyCount>=initRecordNumber/2;
    }
}
