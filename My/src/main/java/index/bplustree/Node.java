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

/**
 *
 B tree index block格式

 - [nextPointer blockNo slotNo key] finalBlockNo ... [header], rootblock记录在indexinfo里面

 - directory header: isLeaf count firstEmptyPointer firstRecordPointer(有序)

 - leaf header: isLeaf count firstEmptyPointer firstRecordPointer(有序) previousBlockNo. nextBlockNo.

 初始,fep=0,frp=order?
 full,fep=order,frp=-1

 */
public abstract class Node {

    public static int Header_Length=24;

    public static int Leaf_Node=1,Directory_Node=0;//isLeaf字段
    public static int End_Block=-1;//用作previous/next block,表示空
    public static int Empty=-1;//用于frp,表示空;或者used flag的最后一个

    public static int Leaf_Offset=FileManager.BLOCK_SIZE-Header_Length;
    public static int Count_Offset=FileManager.BLOCK_SIZE-Header_Length+ 4;
    public static int FEP_Offset =FileManager.BLOCK_SIZE-Header_Length+8;
    public static int FRP_Offset =FileManager.BLOCK_SIZE-Header_Length+12;
    public static int Previous_Offset=FileManager.BLOCK_SIZE-Header_Length+16;
    public static int Final_Offset=FileManager.BLOCK_SIZE-Header_Length+16;
    public static int Next_Offset=FileManager.BLOCK_SIZE-Header_Length+20;

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
        this.count=transaction.getInt(blockId,Count_Offset);
    }

    static Node initNode(Layout indexLayout, int order,
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
        indexSchema.addField(new Field("flag",Field.Integer));
        indexSchema.addField(new Field("blockNo",Field.Integer));
        indexSchema.addField(new Field("slotNo",Field.Integer));
        indexSchema.addField(new Field("key", field.getType(), field.getLen()));
        return new Layout(indexSchema);
    }

    static int calculateOrder(Layout indexLayout){
        return (FileManager.BLOCK_SIZE-Header_Length)/indexLayout.getRecordSize();
    }

    public void close(){
        transaction.unpin(blockId);
    }

    public abstract KeyNodePair insert(Constant<?>key,int blockNumber,int slotNumber);

    public abstract List<Integer> findAll(Constant<?>key);

    public abstract void remove(Constant<?>key,int blockNumber,int slotNumber);

    //下面是辅助方法
    public int getCount() {
        return count;
    }

    public void setCount(int count){
        this.count=count;
        transaction.setInt(blockId,Count_Offset,count,true);
    }

    protected int getFlag(int slotNumber){
        return transaction.getInt(blockId, calculateFlagOffset(slotNumber));
    }

    protected void setFlag(int slotNumber, int value){
        transaction.setInt(blockId, calculateFlagOffset(slotNumber),value,true);
    }

    protected int getBlockNo(int slotNumber){
        return transaction.getInt(blockId, calculateBlockNoOffset(slotNumber));
    }

    protected void setBlockNo(int slotNumber, int value){
        transaction.setInt(blockId, calculateBlockNoOffset(slotNumber),value,true);
    }

    protected int getSlotNo(int slotNumber){
        return transaction.getInt(blockId, calculateSlotNoOffset(slotNumber));
    }

    protected void setSlotNo(int slotNumber, int value){
        transaction.setInt(blockId, calculateSlotNoOffset(slotNumber),value,true);
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

    protected int calculateFlagOffset(int slotNumber){
        return slotNumber*indexLayout.getRecordSize();
    }

    protected int calculateBlockNoOffset(int slotNumber){
        return slotNumber*indexLayout.getRecordSize()+4;
    }

    protected int calculateSlotNoOffset(int slotNumber){
        return slotNumber*indexLayout.getRecordSize()+8;
    }

    protected int calculateKeyOffset(int slotNumber){
        return slotNumber*indexLayout.getRecordSize()+12;
    }

    protected int getFRP(){
        return transaction.getInt(blockId,FRP_Offset);
    }

    protected int getFEP(){
        return transaction.getInt(blockId,FEP_Offset);
    }

    protected void setFRP(int slotNumber){
        transaction.setInt(blockId,FRP_Offset,slotNumber,true);
    }

    protected void setFEP(int slotNumber){
        transaction.setInt(blockId,FEP_Offset,slotNumber,true);
    }

    /**
     * @return 插入的slotNumber
     */
    int insertNext(int prev,int next,Constant<?> key, int blockNumber,int slotNumber){
        int firstEmpty=getFEP();
        int nextEmpty=getFlag(firstEmpty);
        setFEP(nextEmpty);
        if(prev!=Empty){
            setFlag(prev,firstEmpty);
        }
        setKey(firstEmpty,key);
        setFlag(firstEmpty,next);
        setSlotNo(firstEmpty,slotNumber);
        setBlockNo(firstEmpty,blockNumber);

        setCount(getCount()+1);
        return firstEmpty;
    }

    void removeAt(int slotNumber){
        setFlag(slotNumber,getFEP());
        setFEP(slotNumber);

        setCount(getCount()-1);
    }

    Constant<?> split(Node newNode){
        //转移数据,转移order/2条数据,第order/2+1条数据成为split key
        int index=getFRP();
        for(int i=0;i<order/2;i++){
            newNode.setKey(i, getKey(index));
            newNode.setBlockNo(i, getBlockNo(index));
            newNode.setSlotNo(i,getSlotNo(index));
            newNode.setFlag(i,i+1);
            //
            removeAt(index);

            index=getFlag(index);
        }
        Constant<?> splitKey= getKey(index);
        newNode.setFlag(order/2-1,Empty);

        newNode.setFEP(order/2+1);
        newNode.setCount(order/2);
        newNode.setFRP(0);
        //前面已经removeAt,不需要再修改fep,count
        setFRP(index);
        return splitKey;
    }
}
