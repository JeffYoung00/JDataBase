package record;

import file.BlockId;
import predicate.Constant;
import scan.UpdateScan;
import server.Database;
import transaction.Transaction;

public class TableScan implements UpdateScan {

    private String fileName;
    private Transaction transaction;
    private Layout layout;

    private RecordPage recordPage;

    private int currentSlotNumber;

    public TableScan(Transaction transaction,Layout layout,String tableName){
        this.transaction=transaction;
        this.layout=layout;
        this.fileName=tableName+ Database.tablePostfix;

        //初始化
        if(transaction.fileBlockLen(fileName)==0){
            transaction.appendNewFileBlock(fileName);
        }
        recordPage=new RecordPage(transaction,layout,new BlockId(fileName,0));
        currentSlotNumber=-1;
    }

    @Override
    public void beforeFirst() {
        moveToBlock(0);
    }

    @Override
    public void close() {
        recordPage.close();
    }

    public void moveToBlock(int blockNumber){
        recordPage.changeBlock(blockNumber);
        currentSlotNumber=-1;
    }

    public void moveToNewBlock(){
        BlockId blockId=transaction.appendNewFileBlock(fileName);
        moveToBlock(blockId.getBlockNumber());
        recordPage.format();
    }

    public void moveToRecordId(RecordId recordId){
        recordPage.changeBlock(recordId.getBlockNumber());
        currentSlotNumber=recordId.getSlot();
    }

    public RecordId getRecordId(){
        return new RecordId(recordPage.getBlockNumber(),currentSlotNumber);
    }

    //迭代器
    @Override
    public boolean hasNext() {
        currentSlotNumber=recordPage.findSlotAfter(currentSlotNumber,RecordPage.USED);
        while(currentSlotNumber==-1&&recordPage.getBlockNumber()!=transaction.fileBlockLen(fileName)-1){
            moveToBlock(recordPage.getBlockNumber()+1);
            currentSlotNumber=recordPage.findSlotAfter(currentSlotNumber,RecordPage.USED);
        }
        return currentSlotNumber!=-1;
    }

    @Override
    public boolean hasField(String fieldName) {
        return layout.hasField(fieldName);
    }

    @Override
    public Constant<?> getValue(String fieldName){
        return recordPage.getValue(currentSlotNumber,fieldName);
    }

    @Override
    public void setValue(String fieldName, Constant<?> value) {
        recordPage.setValue(currentSlotNumber,fieldName,value,true);
    }

    public void setValue(String fieldName, Constant<?>value,boolean writeToLog){
        recordPage.setValue(currentSlotNumber,fieldName,value,writeToLog);
    }

    //遍历表找空位,没有就新增
    @Override
    public void insert() {
        currentSlotNumber=recordPage.findSlotAfter(currentSlotNumber,RecordPage.EMPTY);
        while(currentSlotNumber==-1){
            if(recordPage.getBlockNumber()!=transaction.fileBlockLen(fileName)-1){
                moveToBlock(recordPage.getBlockNumber()+1);
            }else{
                moveToNewBlock();
            }
            currentSlotNumber=recordPage.findSlotAfter(currentSlotNumber,RecordPage.EMPTY);
        }
        recordPage.setUsed(currentSlotNumber,true);
    }

    @Override
    public void delete() {
        recordPage.setEmpty(currentSlotNumber,true);
    }

    public void getRecordByte(byte[] bytes){
        recordPage.getOriginBytes(currentSlotNumber,bytes);
    }

    public void setRecordByte(byte[] bytes){
        recordPage.setOriginBytes(currentSlotNumber,bytes);
    }
}
