package index.bplustree;

import file.BlockId;
import index.IndexScan;
import metadata.IndexInfo;
import predicate.Constant;
import record.Layout;
import record.RecordId;
import record.TableScan;
import server.Database;
import server.DatabaseException;
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

    //
    Constant<?> key;
    List<Integer> lastSearch;
    int lastSearchPos;

    int previousBlock=0;



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
            root=Node.createNode(indexLayout,order,transaction,new BlockId(fileName, indexInfo.getRootBlockNumber()));
        }
    }


    @Override
    public void beforeFirst(Constant<?> key) {
        this.key=key;
        this.lastSearch=root.findAll(key);
        //排序,防止一个block中有多个相同key,每次都只找到第一个key
        this.lastSearch.sort((a,b)->a-b);
        this.lastSearchPos=0;
    }

    @Override
    public boolean hasNext() {
        if(lastSearchPos>=lastSearch.size()){
            return false;
        }
        int nextBlock=lastSearch.get(lastSearchPos);
        if(nextBlock!=previousBlock){
            previousBlock=nextBlock;
            tableScan.moveToBlock(nextBlock);
        }
        while(tableScan.hasNext()){
            if(tableScan.getValue(indexInfo.getField().getName()).equals(key)){
                return true;
            }
        }
        throw new DatabaseException("index error, equal key not found through index");
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
        KeyNodePair keyNodePair =root.insert(key,recordId.getBlockNumber());
        if(keyNodePair==null){
            return;
        }
        BlockId newBlockId=transaction.appendNewFileBlock(fileName);

        DirectoryNode newRoot=new DirectoryNode(indexLayout,order,transaction,newBlockId);

        newRoot.setKey(0,keyNodePair.getKey());
        newRoot.setBlockNumber(0,indexInfo.getRootBlockNumber());
        newRoot.setBlockNumber(1,keyNodePair.getBlockNumber());

        //todo 修改写入metadata
        indexInfo.setRootBlockNumber(newBlockId.getBlockNumber());
        indexInfo.setIndexHeight(indexInfo.getIndexHeight()+1);

        //close
        root.close();

        root=newRoot;
    }

    @Override
    public void delete(Constant<?>key,RecordId recordId) {
        root.remove(key,recordId.getBlockNumber());
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
