package record;

import file.BlockId;
import file.FileManager;
import predicate.Constant;
import server.DatabaseException;
import transaction.Transaction;

/**
 * 计算offset/flag,屏蔽实际的存储方式
 */
public class RecordPage {
    public static final int EMPTY = 0, USED = 1;
    public static final int Flag_Size=4;

    Transaction transaction;
    Layout layout;
    BlockId blockId;

    private int maxRecord;

    public RecordPage(Transaction transaction, Layout layout, BlockId blockId){
        this.transaction=transaction;
        this.layout=layout;
        this.blockId=blockId;
        transaction.pin(blockId);

        maxRecord= FileManager.BLOCK_SIZE/(layout.getRecordSize()+Flag_Size);
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

    public Constant<?> getValue(int slotNumber,String fieldName){
        int type=layout.getType(fieldName);
        if(type== Field.Integer){
            return new Constant<>(transaction.getInt(blockId,calculateOffset(slotNumber,fieldName)));
        }else{
            return new Constant<>(transaction.getString(blockId, calculateOffset(slotNumber, fieldName)));
        }
    }

    public void setValue(int slotNumber,String fieldName,Constant<?>value,boolean writeToLog){
        int type=layout.getType(fieldName);
        if(type==Field.Integer){
            transaction.setInt(blockId,calculateOffset(slotNumber,fieldName),(Integer) value.getValue(),writeToLog);
        }else if(type==Field.String){
            transaction.setString(blockId,calculateOffset(slotNumber,fieldName),(String) value.getValue(),writeToLog);
        }
    }

    public void setEmpty(int slotNumber,boolean log){
        transaction.setInt(blockId,calculateOffset(slotNumber),EMPTY,log);
    }

    public void setUsed(int slotNumber,boolean log){
        transaction.setInt(blockId,calculateOffset(slotNumber),USED,log);
    }

    /**
     * 在当前block的slot number后找一个empty/used slot
     * @param slotNumber
     * @return empty slot number, 没找到返回-1
     */
    public int findSlotAfter(int slotNumber,int flag){
        slotNumber++;
        while( slotNumber<maxRecord){
            if(isFlag(slotNumber,flag)){
                return slotNumber;
            }
            slotNumber++;
        }
        return -1;
    }

    private boolean isFlag(int slotNumber,int flag){
        int f=transaction.getInt(blockId,calculateOffset(slotNumber));
        if(f!=0&&f!=1){
            throw new DatabaseException("wrong flag in "+blockId+slotNumber);
        }
        return f==flag;
    }

    public int getBlockNumber(){
        return blockId.getBlockNumber();
    }

    //没有null,format只改了flag位,insert/delete也没有format
    public void format(){
        int slotNumber=0;
        while( slotNumber<maxRecord){
            setEmpty(slotNumber,false);
            slotNumber++;
        }
    }

    private int calculateOffset(int slotNumber){
        return (layout.getRecordSize()+Flag_Size)*slotNumber;
    }

    private int calculateOffset(int slotNumber,String fieldName){
        return (layout.getRecordSize()+Flag_Size)*slotNumber+layout.getOffset(fieldName)+Flag_Size;
    }

    public void getOriginBytes(int slotNumber,byte[]bytes){
        transaction.getOriginBytes(blockId,calculateOffset(slotNumber),bytes);
    }

    public void setOriginBytes(int slotNumber,byte[]bytes){
        transaction.setOriginBytes(blockId,calculateOffset(slotNumber),bytes);
    }

    public static int recordPerBlock(Schema schema){
        return FileManager.BLOCK_SIZE/ (Layout.countRecordSize(schema)+Flag_Size);
    }
}
