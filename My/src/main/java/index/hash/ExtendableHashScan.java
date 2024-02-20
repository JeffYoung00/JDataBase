package index.hash;

import file.BlockId;
import index.IndexScan;
import metadata.IndexInfo;
import record.*;
import predicate.Constant;
import server.Database;
import transaction.Transaction;

import java.util.ArrayList;
import java.util.List;

//todo 修改写回metadata
public class ExtendableHashScan implements IndexScan {

    private Transaction transaction;
    private TableScan tableScan;
    private IndexInfo indexInfo;
    private String dataFileName;
    private String bucketFileName;

    private int globalDepth;

    DataPage dataPage;
    BucketPage bucketPage;

    public ExtendableHashScan(Transaction transaction, IndexInfo indexInfo,TableScan tableScan){
        this.transaction=transaction;
        this.indexInfo=indexInfo;
        this.tableScan=tableScan;
        this.dataFileName =indexInfo.getIndexName()+Database.hashIndexPostfix;
        this.bucketFileName=indexInfo.getIndexName()+ Database.hashBucketPostfix;

        this.globalDepth=indexInfo.getGlobalDepth();

        int fileBlockLen = transaction.fileBlockLen(bucketFileName);
        if(fileBlockLen==0){
            transaction.appendNewFileBlock(dataFileName);
            transaction.appendNewFileBlock(bucketFileName);
            //format,应为最初的depth是0,所以不需要format bucket
            DataPage dataPage=new DataPage(transaction,new BlockId(dataFileName,0),0);
            dataPage.close();
        }
        bucketPage=new BucketPage(transaction,indexInfo);
    }

    //todo
    public static int hashcode(Constant<?>key){
        return key.getValue().hashCode();
    }

    List<Integer> lastSearch;
    int lastSearchPos;
    Constant<?> lastKey;

    @Override
    public void beforeFirst(Constant<?> searchKey) {
        lastKey=searchKey;
        int hashcode=hashcode(searchKey);
        int blockNo = bucketPage.getBlockNoByHash(hashcode);
        dataPage.changeBlock(blockNo);
        lastSearch=dataPage.findAll(hashcode);
        lastSearchPos=0;
    }

    @Override
    public boolean hasNext() {
        if(lastSearchPos>=lastSearch.size()){
            return false;
        }
        tableScan.moveToBlock(lastSearch.get(lastSearchPos++));
        tableScan.moveToSlot(lastSearch.get(lastSearchPos++));
        if(!tableScan.getValue(indexInfo.getField().getName()).equals(lastKey)){
            return hasNext();
        }
        return true;
    }

    @Override
    public RecordId getRecordId() {
        return tableScan.getRecordId();
    }

    @Override
    public void close() {
        bucketPage.close();
        dataPage.close();
    }

    @Override
    public void insert(Constant<?> key, RecordId recordId) {
        int hashcode=hashcode(key);
        int blockNo = bucketPage.getBlockNoByHash(hashcode);
        dataPage.changeBlock(blockNo);
        DataPage newNode=dataPage.insert(hashcode,recordId.getBlockNumber(),recordId.getSlot());
        //是否global split在bucket里面判断
        bucketPage.localSplit(hashcode,newNode.blockId.getBlockNumber(),newNode.localDepth);
    }

    @Override
    public void delete(Constant<?> key, RecordId recordId) {
        int hashcode=hashcode(key);
        int blockNo = bucketPage.getBlockNoByHash(hashcode);
        dataPage.changeBlock(blockNo);
        dataPage.remove(hashcode,recordId.getBlockNumber(),recordId.getSlot());
    }

    @Override
    public Constant<?> getValue(String fieldName) {
        return tableScan.getValue(fieldName);
    }

    @Override
    public boolean hasField(String fieldName) {
        return tableScan.hasField(fieldName);
    }
}
