package index.bplustree;

import file.BlockId;
import predicate.Constant;
import record.Layout;
import transaction.Transaction;

import java.util.List;

public class DirectoryNode extends Node {


    public DirectoryNode(Layout indexLayout,int order,
                         Transaction transaction, BlockId blockId){
        super(indexLayout,order,transaction,blockId);
    }

    //新的block,format
    public DirectoryNode(Layout indexLayout,int order,
                    Transaction transaction, BlockId blockId,
                    int firstBlockNumber,int finalBlockNumber){
        this(indexLayout,order,transaction,blockId);
        transaction.setInt(blockId,Leaf_Offset,Leaf_Node,false);
        //有一条记录,所以第一个empty是1,count是1
        transaction.setInt(blockId,Count_Offset,1,false);
        transaction.setInt(blockId,FEP_Offset,1,false);
        transaction.setInt(blockId,FRP_Offset,0,false);
        transaction.setInt(blockId,Final_Offset,finalBlockNumber,false);
        transaction.setInt(blockId, calculateFlagOffset(0),firstBlockNumber,false);
        //从1开始
        for(int i=1;i<order;i++){
            transaction.setInt(blockId,i*calculateFlagOffset(i),i+1,false);
        }
    }

    //这两个字段用于find之后分裂
    int index;
    int prev;
    @Override
    public KeyNodePair insert(Constant<?> key, int blockNumber,int slotNumber) {
        /**
         * 死锁避免,有split风险时需要加x锁
         */
        if(getCount()==order-1){
            transaction.xLockForBPlusTree(blockId);
        }else{
            transaction.releaseXLockForBPlusTree();
        }

        //第一个node>=key,或者最后一个node
        Node sonNode=findSonNode(key);
        KeyNodePair keyNodePair=sonNode.insert(key,blockNumber,slotNumber);

        sonNode.close();

        //son don't split
        if(keyNodePair==null){
            return null;
        }
        //dir don't split
        insertNext(prev,index,key,blockNumber,0);
        if(getCount()!= order){
            return null;
        }
        //dir split
        BlockId newBlockId=transaction.appendNewFileBlock(blockId.getFileName());
        DirectoryNode newNode=new DirectoryNode(indexLayout,order,transaction,newBlockId,keyNodePair.getFirstBlockNumber(),sonNode.blockId.getBlockNumber());
        Constant<?>splitKey=split(newNode);
        return new KeyNodePair(splitKey,newBlockId.getBlockNumber());
    }

    @Override
    public List<Integer> findAll(Constant<?> key) {
        Node sonNode=findSonNode(key);
        List<Integer> ret=sonNode.findAll(key);
        sonNode.close();
        return ret;
    }

    @Override
    public void remove(Constant<?> key, int blockNumber,int slotNumber) {
        Node sonNode=findSonNode(key);
        sonNode.remove(key,blockNumber,slotNumber);
        sonNode.close();
    }

    private Node findSonNode(Constant<?>key){
        prev=Empty;
        index=getFRP();
        while(index!=Empty && getKey(index).compareTo(key)<0){
            prev=index;
            index=getFlag(index);
        }
        BlockId sonBlock=new BlockId(blockId.getFileName(), getFlag(index));
        return Node.initNode(indexLayout,order,transaction,sonBlock);
    }
}
