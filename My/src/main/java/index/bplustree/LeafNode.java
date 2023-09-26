package index.bplustree;

import file.BlockId;
import predicate.Constant;
import record.Layout;
import transaction.Transaction;

import java.util.ArrayList;
import java.util.List;

/**
 *  isLeaf count pre next
 */

public class LeafNode extends Node {

    static int Previous_Offset=8;
    static int Next_Offset=12;

    int previousBlockNumber;
    int nextBlockNumber;

    public LeafNode(Layout indexLayout,int order,
            Transaction transaction, BlockId blockId){
        super(indexLayout,order,transaction,blockId);

        this.previousBlockNumber=transaction.getInt(blockId,8);
        this.nextBlockNumber=transaction.getInt(blockId,12);
    }

    //新的block,format
    public LeafNode(Layout indexLayout,int order,
                    Transaction transaction, BlockId blockId,
                    int previousBlockNumber,int nextBlockNumber){
        super(indexLayout,order,transaction,blockId);

        this.previousBlockNumber=previousBlockNumber;
        this.nextBlockNumber=nextBlockNumber;

        transaction.setInt(blockId,Previous_Offset,previousBlockNumber,false);
        transaction.setInt(blockId,Next_Offset,nextBlockNumber,false);
        transaction.setInt(blockId,4,0,false);
        transaction.setInt(blockId,0,Leaf_Node,false);
    }


    @Override
    public KeyNodePair insert(Constant<?> key, int blockNumber) {

        //放在<=的后面
        int index=0;
        while( getKey(index).compareTo(key)<=0 ){
            index++;
        }

        for(int i=getCount()-1;i>=index;i--){
            setKey(i+1, getKey(i));
        }
        for(int i=getCount()-1;i>=index;i--){
            setKey(i+1, getKey(i));
        }

        /**/
        setCount(getCount()+1);

        if(getCount()!= order){
            return null;
        }
        //split
        int midIndex= order /2;

        Constant<?> splitKey= getKey(midIndex);

        //修改链表
        BlockId newBlockId=transaction.appendNewFileBlock(blockId.getFileName());
        LeafNode newNode=new LeafNode(indexLayout,order,transaction,newBlockId,blockId.getBlockNumber(),nextBlockNumber);
        transaction.setInt(blockId,Next_Offset,newBlockId.getBlockNumber(),true);

        for(int i=0;i<order-midIndex;i++){
            newNode.setKey(i, getKey(midIndex+i));
        }
        for(int i=0;i<order-midIndex;i++){
            newNode.setKey(i, getKey(midIndex+i));
        }
        setCount(midIndex);
        return new KeyNodePair(splitKey,newBlockId.getBlockNumber());
    }

    @Override
    public List<Integer> findAll(Constant<?> key) {
        List<Integer> ret=new ArrayList<>();
        int i=0;
        while(i<getCount()&& key.compareTo(getKey(i))<0 ){
            i++;
        }
        //大于所有stored key
        if(i==getCount()){
            return null;
        }
        //加入所有元素
        while(i<getCount()&&key.equals(getKey(i))){
            ret.add(i);
            i++;
        }
        //找到leaf的末尾,说明下一个结点也可能有
        if(i==getCount()){
            BlockId nextBlock=new BlockId(blockId.getFileName(),blockId.getBlockNumber()+1);
            ret.addAll( new LeafNode(indexLayout,order,transaction,nextBlock).findAll(key) );
        }
        return ret;
    }

    @Override
    public void remove(Constant<?> key, int blockNumber) {
        int index=0;
        while(index<getCount()&&key.compareTo(getKey(index))<0){
            index++;
        }
        //没找到
        if(index==getCount()){
            return;
        }
        //相等就找到,否则没找到
        if(key.equals(getKey(index))){
            for(int i=index+1;i<getCount();i++){
                setKey(i-1, getKey(i));
                setBlockNumber(i-1,getBlockNumber(i));
            }
        }
    }
}
