package index.bplustree;

import file.BlockId;
import predicate.Constant;
import record.Layout;
import transaction.Transaction;
import transaction.concurrency.Concurrency;

import java.util.ArrayList;
import java.util.List;

/**
 *  isLeaf count pre next
 */

public class LeafNode extends Node {

    int previousBlockNumber;
    int nextBlockNumber;

    public LeafNode(Layout indexLayout,int order,
            Transaction transaction, BlockId blockId){
        super(indexLayout,order,transaction,blockId);

        this.previousBlockNumber=transaction.getInt(blockId,previousBlockNumber);
        this.nextBlockNumber=transaction.getInt(blockId,nextBlockNumber);
    }

    //新的block,format
    public LeafNode(Layout indexLayout,int order,
                    Transaction transaction, BlockId blockId,
                    int previousBlockNumber,int nextBlockNumber){
        this(indexLayout,order,transaction,blockId);
        transaction.setInt(blockId,Leaf_Offset,Leaf_Node,false);
        transaction.setInt(blockId,Count_Offset,0,false);
        transaction.setInt(blockId,FEP_Offset,0,false);
        transaction.setInt(blockId,FRP_Offset,-1,false);
        transaction.setInt(blockId,Previous_Offset,previousBlockNumber,false);
        transaction.setInt(blockId,Next_Offset,nextBlockNumber,false);
        for(int i=0;i<order;i++){
            transaction.setInt(blockId,i*calculateFlagOffset(i),i+1,false);
        }
    }

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

        //第一个点
        int firstSlot = getFRP();

        //empty或者新key最小,需要更新frp
        if(firstSlot==Empty||getKey(firstSlot).compareTo(key)>0){
            //修改frp
            setFRP(insertNext(Empty,firstSlot,key,blockNumber,slotNumber));
            return null;
        }

        //next是插入点的下一个
        int prev=firstSlot;
        int next=getFlag(firstSlot);
        while(next!=Empty&&getKey(next).compareTo(key)<=0){
            prev=next;
            next=getFlag(next);
        }
        insertNext(prev,next,key,blockNumber,slotNumber);

        //no split
        if(getCount()!= order){
            return null;
        }

        //修改链表
        BlockId newBlockId=transaction.appendNewFileBlock(blockId.getFileName());
        LeafNode newNode=new LeafNode(indexLayout,order,transaction,newBlockId,previousBlockNumber,blockId.getBlockNumber());
        setPreviousBlockNumber(newBlockId.getBlockNumber());
        //split
        Constant<?>splitKey=split(newNode);
        return new KeyNodePair(splitKey,newBlockId.getBlockNumber());
    }

    //返回 [blockNo slotNo]
    @Override
    public List<Integer> findAll(Constant<?> key) {
        List<Integer> ret=new ArrayList<>();
        int i=getFRP();
        while(i!=Empty&& key.compareTo(getKey(i))<0 ){
            i=getFlag(i);
        }
        //大于所有stored key
        if(i==Empty){
            return null;
        }
        //加入所有元素
        while(i!=Empty&&key.equals(getKey(i))){
            ret.add(getBlockNo(i));
            ret.add(getSlotNo(i));
            i=getFlag(i);
        }
        //找到leaf的末尾,说明下一个结点也可能有
        if(i==getCount()&&nextBlockNumber!=End_Block){
            BlockId nextBlock=new BlockId(blockId.getFileName(),nextBlockNumber);
            ret.addAll( new LeafNode(indexLayout,order,transaction,nextBlock).findAll(key) );
        }
        return ret;
    }

    @Override
    public void remove(Constant<?> key, int blockNumber,int slotNumber) {
        int index=getFRP();
        if(index==Empty){
            return;
        }
        //修改frp
        if(key.compareTo(getKey(index))==0){
            removeAt(index);
            setFRP(getFlag(index));
        }

        while(index!=Empty&&key.compareTo(getKey(index))<=0){
            if(getBlockNo(index)==blockNumber&&getSlotNo(index)==slotNumber){
                removeAt(index);
                return;
            }
            index=getFlag(index);
        }
    }

    void setPreviousBlockNumber(int prev){
        this.previousBlockNumber=prev;
        transaction.setInt(blockId,Previous_Offset,prev,true);
    }
}
