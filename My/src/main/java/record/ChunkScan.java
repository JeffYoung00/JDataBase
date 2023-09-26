package record;

import file.BlockId;
import predicate.Constant;
import scan.Scan;
import transaction.Transaction;

import java.util.ArrayList;
import java.util.List;

/**
 * chunk scan 和table scan不同, 不能移动到初始化之后的块中, 因为这个类的意义就是为了不额外io
 * [start,end)
 */
public class ChunkScan implements Scan {

    private Transaction transaction;
    private Layout layout;
    private String fileName;
    private int startBlockNumber,endBlockNumber;

    private int currentBlockNumber;
    private int currentSlotNumber;

    private List<RecordPage> recordPages=new ArrayList<>();

    //可能是temp table用,也可能是table用,所以直接传filename
    public ChunkScan(Transaction transaction, Layout layout, String fileName, int startBlockNumber,int endBlockNumber){
        this.transaction=transaction;
        this.layout=layout;
        this.fileName=fileName;
        this.startBlockNumber=startBlockNumber;
        this.endBlockNumber=endBlockNumber;

        for(int i=startBlockNumber;i<endBlockNumber;i++){
            BlockId blockId=new BlockId(fileName,i);
            recordPages.add( new RecordPage(transaction,layout,blockId) );
        }
    }

    @Override
    public void beforeFirst() {
        moveToBlock(startBlockNumber);
    }

    @Override
    public boolean hasNext() {
        RecordPage recordPage=recordPages.get(currentBlockNumber-startBlockNumber);
        currentSlotNumber=recordPage.findSlotAfter(currentSlotNumber,RecordPage.USED);
        while(currentSlotNumber==-1&&recordPage.getBlockNumber()!=endBlockNumber-1){
            moveToBlock(recordPage.getBlockNumber()+1);
            currentSlotNumber=recordPage.findSlotAfter(currentSlotNumber,RecordPage.USED);
        }
        return currentSlotNumber!=-1;
    }

    @Override
    public Constant<?> getValue(String fieldName) {
        return recordPages.get(currentBlockNumber-startBlockNumber).getValue(currentSlotNumber,fieldName);
    }

    @Override
    public boolean hasField(String fieldName) {
        return layout.hasField(fieldName);
    }

    @Override
    public void close() {
        for(RecordPage recordPage:recordPages){
            recordPage.close();
        }
    }

    public void moveToBlock(int blockNumber){
        currentSlotNumber=-1;
        currentBlockNumber=blockNumber;
    }
}
