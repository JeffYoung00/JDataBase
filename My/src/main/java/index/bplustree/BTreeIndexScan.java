package index.bplustree;

import file.BlockId;
import index.IndexScan;
import metadata.IndexInfo;
import predicate.Constant;
import record.Layout;
import record.RecordId;
import record.TableScan;
import server.Database;
import transaction.Transaction;

import java.util.List;

public class BTreeIndexScan implements IndexScan {

    TableScan tableScan;
    IndexInfo indexInfo;
    String fileName;

    Layout indexLayout;
    Transaction transaction;
    int order;
    Node root;

    Constant<?> key;
    //查询得到的结果
    List<Integer> lastSearch;
    int lastSearchPos;

    public BTreeIndexScan(Transaction transaction, IndexInfo indexInfo, TableScan tableScan){
        this.fileName=indexInfo.getIndexName()+Database.bTreeIndexPostfix;
        this.indexLayout=Node.indexLayout(indexInfo.getField());
        this.order=Node.calculateOrder(indexLayout);

        this.transaction=transaction;
        this.tableScan=tableScan;
        this.indexInfo=indexInfo;

        //
        int fileBlockLen = transaction.fileBlockLen(fileName);
        if(fileBlockLen==0){
            BlockId firstBlock=transaction.appendNewFileBlock(fileName);
            root=new LeafNode(indexLayout,order,transaction,firstBlock,Node.End_Block,Node.End_Block);
        }else{
            root=Node.initNode(indexLayout,order,transaction,new BlockId(fileName, indexInfo.getRootBlockNumber()));
        }
    }


    @Override
    public void beforeFirst(Constant<?> key) {
        this.key=key;
        this.lastSearch=root.findAll(key);
        this.lastSearchPos=0;
    }

    @Override
    public boolean hasNext() {
        if(lastSearchPos>=lastSearch.size()){
            return false;
        }
        tableScan.moveToBlock(lastSearch.get(lastSearchPos++));
        tableScan.moveToSlot(lastSearch.get(lastSearchPos++));
        return true;
    }

    @Override
    public RecordId getRecordId() {
        return tableScan.getRecordId();
    }

    @Override
    public void close() {
        root.close();
        tableScan.close();
    }

    @Override
    public void insert(Constant<?> value, RecordId recordId) {
        KeyNodePair keyNodePair =root.insert(key,recordId.getBlockNumber(),recordId.getSlot());
        if(keyNodePair==null){
            return;
        }
        BlockId newBlockId=transaction.appendNewFileBlock(fileName);
        DirectoryNode newRoot=new DirectoryNode(indexLayout,order,transaction,newBlockId,keyNodePair.getFirstBlockNumber(),
                indexInfo.getRootBlockNumber());

        //todo 修改写入metadata
        indexInfo.setRootBlockNumber(newBlockId.getBlockNumber());
        indexInfo.setIndexHeight(indexInfo.getIndexHeight()+1);

        //close
        root.close();
        root=newRoot;
    }

    @Override
    public void delete(Constant<?>key,RecordId recordId) {
        root.remove(key,recordId.getBlockNumber(),recordId.getSlot());
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
