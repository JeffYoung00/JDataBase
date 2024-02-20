package index.hash;

import file.BlockId;
import file.FileManager;
import predicate.Constant;
import record.Field;
import record.Layout;
import record.Schema;
import transaction.Transaction;

import java.util.ArrayList;

/**
 * hash index block data: [usedFlag/nextPointer blockNo. slotNo.  hash]... count firstEmptyPointer localDepth
 *
 * - 这里不记录key, 直接比较hash
 */

public class DataPage {

    static int Header_Length=12;
    static int Count_Offset= FileManager.BLOCK_SIZE-Header_Length;
    static int FEP_Offset= FileManager.BLOCK_SIZE-Header_Length+4;
    static int Depth_Offset= FileManager.BLOCK_SIZE-Header_Length+8;

    static int IndexRecordSize=16;
    static int DataOrder=(FileManager.BLOCK_SIZE-Header_Length)/IndexRecordSize;

    static int Flag_Used;//用于flag,表示used,非used的flag就是next empty pointer

    static Layout indexLayout;
    static{
        Schema indexSchema=new Schema();
        indexSchema.addField(new Field("flag",Field.Integer));
        indexSchema.addField(new Field("blockNumber",Field.Integer));
        indexSchema.addField(new Field("slotNumber",Field.Integer));
        indexSchema.addField(new Field("hash",Field.Integer));
        indexLayout=new Layout(indexSchema);
    }

    Transaction transaction;
    BlockId blockId;
    int localDepth;
    int mask;

    public DataPage(Transaction transaction, BlockId blockId){
        transaction.pin(blockId);
        this.transaction=transaction;
        this.blockId=blockId;
        this.localDepth=transaction.getInt(blockId,Depth_Offset);
        mask=1<<localDepth-1;
    }

    //format
    public DataPage(Transaction transaction,BlockId blockId,int localDepth){
        transaction.pin(blockId);
        this.transaction=transaction;
        this.blockId=blockId;
        this.localDepth=localDepth;
        for(int i=0;i<DataOrder;i++){
            transaction.setInt(blockId,calculateFlagOffset(i),i+1,false);
        }
        transaction.setInt(blockId,Count_Offset,0,false);
        transaction.setInt(blockId,FEP_Offset,0,false);
        transaction.setInt(blockId,Depth_Offset,localDepth,false);
    }

    public void close(){
        transaction.unpin(blockId);
    }

    public void changeBlock(int blockNumber){
        if(blockNumber== blockId.getBlockNumber()){
            return;
        }
        close();
        blockId=new BlockId(blockId.getFileName(), blockNumber);
        transaction.pin(blockId);
    }

    int getFEP(){
        return transaction.getInt(blockId,FEP_Offset);
    }

    void setFEP(int slotNumber){
        transaction.setInt(blockId,FEP_Offset,slotNumber,true);
    }

    int getCount(){
        return transaction.getInt(blockId,Count_Offset);
    }

    void setCount(int count){
        transaction.setInt(blockId,Count_Offset,count,true);
    }

    int calculateFlagOffset(int slotNumber){
        return slotNumber*IndexRecordSize;
    }
    int calculateBlockNoOffset(int slotNumber){
        return slotNumber*IndexRecordSize+4;
    }
    int calculateSlotNoOffset(int slotNumber){
        return slotNumber*IndexRecordSize+8;
    }
    int calculateHashOffset(int slotNumber){
        return slotNumber*IndexRecordSize+12;
    }

    int getFlag(int slotNumber){
        return transaction.getInt(blockId,calculateFlagOffset(slotNumber));
    }
    int getHash(int slotNumber){
        return transaction.getInt(blockId,calculateHashOffset(slotNumber));
    }
    int getBlockNo(int slotNumber){
        return transaction.getInt(blockId,calculateBlockNoOffset(slotNumber));
    }
    int getSlotNo(int slotNumber){
        return transaction.getInt(blockId,calculateSlotNoOffset(slotNumber));
    }
    void setFlag(int slotNumber,int flag){
        transaction.setInt(blockId,calculateFlagOffset(slotNumber),flag,true);
    }
    void setHash(int slotNumber,int hash){
        transaction.setInt(blockId,calculateHashOffset(slotNumber),hash,true);
    }
    void setBlockNo(int slotNumber,int blockNumber){
        transaction.setInt(blockId,calculateBlockNoOffset(slotNumber),blockNumber,true);
    }
    void setSlotNo(int slotNumber,int slotNo){
        transaction.setInt(blockId,calculateSlotNoOffset(slotNumber),slotNo,true);
    }

    public DataPage insert(int hash, int blockNumber, int slotNumber){
        int empty=getFEP();
        int nextEmpty=getFlag(empty);
        setFEP(nextEmpty);
        setFlag(empty,Flag_Used);
        setHash(empty,hash);
        setBlockNo(empty,blockNumber);
        setSlotNo(empty,slotNumber);
        setCount(getCount()+1);

        if(getCount()==DataOrder){
            DataPage newNode=new DataPage(transaction,transaction.appendNewFileBlock(blockId.getFileName()),localDepth+1);
            transaction.setInt(blockId,Depth_Offset,localDepth+1,true);
            localDepth++;
            for(int i=0;i<DataOrder;i++){
                if(getFlag(i)==Flag_Used && isOneInNthBit(getHash(i)) ){
                    newNode.insert(getHash(i),getBlockNo(i),getSlotNo(i));
                    removeAt(i);
                }
            }
            return newNode;
        }
        return null;
    }

    //判断是否第localDepth-1 bit是1,即需要分出
    boolean isOneInNthBit(int hashCode){
        return ((hashCode>>>(localDepth-1))&1)==1;
    }

    public void remove(int hash, int blockNumber, int slotNumber){
        for(int i=0;i<DataOrder;i++){
            if(getFlag(i)==Flag_Used && getHash(i)==hash && getBlockNo(i)==blockNumber && getSlotNo(i)==slotNumber ){
                removeAt(i);
                return;
            }
        }
    }

    void removeAt(int slotNumber){
        setFlag(slotNumber,getFEP());
        setFEP(slotNumber);
        setCount(getCount()+1);
    }

    public ArrayList<Integer> findAll(int hash){
        ArrayList<Integer> ret=new ArrayList<>();
        for(int i=0;i<DataOrder;i++){
            if(getFlag(i)==Flag_Used && getHash(i)==hash){
                ret.add(getBlockNo(i));
                ret.add(getSlotNo(i));
            }
        }
        return ret;
    }
}
