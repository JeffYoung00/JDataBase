package index.bplustree;

import file.BlockId;
import file.FileManager;
import predicate.Constant;
import record.Field;
import record.Layout;
import record.Schema;
import server.DatabaseException;
import transaction.Transaction;

import java.util.List;

/**
 * 叶子结点和非叶子结点的通用方法
 * 屏蔽类型,offset计算
 */
public abstract class Node {

    public static int Leaf_Node=1,Directory_Node=0;
    public static int Header_Length=20;
    public static int End_Block=-1;//用作previous next block,表示空


    Layout indexLayout;
    Transaction transaction;
    BlockId blockId;
    int order;

    int keyType;
    private int count;

    Node(Layout indexLayout,int order,Transaction transaction, BlockId blockId){
        this.indexLayout=indexLayout;
        this.transaction=transaction;
        this.blockId=blockId;
        this.order=order;

        this.keyType=indexLayout.getType("key");
        this.count=transaction.getInt(blockId,4);
    }

    static Node createNode(Layout indexLayout,int order,
                           Transaction transaction, BlockId blockId){
        transaction.pin(blockId);
        int isLeaf=transaction.getInt(blockId,0);
        if(isLeaf==Leaf_Node){
            return new LeafNode(indexLayout,order,transaction,blockId);
        }else if(isLeaf==Directory_Node){
            return new DirectoryNode(indexLayout,order,transaction,blockId);
        }else{
            throw new DatabaseException("index node error type");
        }

    }

    static Layout indexLayout(Field field){
        Schema indexSchema=new Schema();
        indexSchema.addField(new Field("key", field.getType(), field.getLen()));
        indexSchema.addField(new Field("blockNumber",Field.Integer));
        return new Layout(indexSchema);
    }

    static int calculateOrder(Layout indexLayout){
        return (FileManager.BLOCK_SIZE-Header_Length+4)/indexLayout.getRecordSize();
    }

    public void close(){
        transaction.unpin(blockId);
    }

    public abstract KeyNodePair insert(Constant<?>key,int blockNumber);

    public abstract List<Integer> findAll(Constant<?>key);

    public abstract void remove(Constant<?>key,int blockNumber);

    //下面是辅助方法


    public int getCount() {
        return count;
    }

    public void setCount(int count){
        this.count=count;
        transaction.setInt(blockId,4,count,true);
    }

    protected int getBlockNumber(int slotNumber){
        return transaction.getInt(blockId, calculateBlockNumberOffset(slotNumber));
    }

    protected void setBlockNumber(int slotNumber,int value){
        transaction.setInt(blockId,calculateBlockNumberOffset(slotNumber),value,true);
    }

    protected Constant<?> getKey(int slotNumber){
        if(keyType==Field.Integer){
            return new Constant<>(transaction.getInt(blockId,calculateKeyOffset(slotNumber)));
        }else{
            return new Constant<>(transaction.getString(blockId,calculateKeyOffset(slotNumber)));
        }
    }

    protected void setKey(int slotNumber, Constant<?>value){
        if(keyType==Field.Integer){
            transaction.setInt(blockId,calculateKeyOffset(slotNumber),(Integer)value.getValue(),true);
        }else{
            transaction.setString(blockId,calculateKeyOffset(slotNumber),(String)value.getValue(),true);
        }
    }

    protected int calculateBlockNumberOffset(int slotNumber){
        return slotNumber*indexLayout.getRecordSize()+Header_Length;
    }

    protected int calculateKeyOffset(int slotNumber){
        return slotNumber*indexLayout.getRecordSize()+Header_Length+4;
    }

}
