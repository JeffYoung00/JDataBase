package index.bplustree;

import file.BlockId;
import predicate.Constant;
import record.Layout;
import transaction.Transaction;

import java.util.List;

//todo 注意close
public class DirectoryNode extends Node {

    int index;

    public DirectoryNode(Layout indexLayout,int order,
                         Transaction transaction, BlockId blockId){
        super(indexLayout,order,transaction,blockId);
    }

    @Override
    public KeyNodePair insert(Constant<?> key, int blockNumber) {
        //第一个node>=key,或者最后一个node
        Node sonNode=findSonNode(key);
        KeyNodePair keyNodePair=sonNode.insert(key,blockNumber);

        sonNode.close();

        if(keyNodePair==null){
            return null;
        }
        //son split,注意index和index+1
        for(int i=getCount()-1;i>=index;i--){
            setKey(i+1, getKey(i));
            setBlockNumber(i+2,getBlockNumber(i+1));
        }
        setKey(index,keyNodePair.getKey());
        setBlockNumber(index+1,keyNodePair.getBlockNumber());

        /**/
        setCount(getCount()+1);

        if(getCount()!= order){
            return null;
        }
        //split
        int midIndex= order /2;
        BlockId newBlockId=transaction.appendNewFileBlock(blockId.getFileName());
        DirectoryNode newNode=new DirectoryNode(indexLayout,order,transaction,newBlockId);

        //keys留[0,midIndex),上升split key,分裂[midIndex+1,m)
        Constant<?> splitKey= getKey(midIndex);
        for(int i=0;i<order-midIndex-1;i++){
            newNode.setKey(i, getKey(i+midIndex+1));
        }
        for(int i=0;i<order;i++){
            newNode.setBlockNumber(i,getBlockNumber(i+midIndex+1));
        }

        newNode.close();
        setCount(midIndex);
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
    public void remove(Constant<?> key, int blockNumber) {
        Node sonNode=findSonNode(key);
        sonNode.remove(key,blockNumber);
        sonNode.close();
    }

    private Node findSonNode(Constant<?>key){
        index=0;
        while(index<getCount()&& getKey(index).compareTo(key)<0){
            index++;
        }
        BlockId sonBlock=new BlockId(blockId.getFileName(),getBlockNumber(index));
        return Node.createNode(indexLayout,order,transaction,sonBlock);
    }
}
