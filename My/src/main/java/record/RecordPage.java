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
    /**
     * used flag==-1
     * empty flag==next empty
     * last flag==empty head
     * full:last flag==max
     * insert last flag,last flag=empty head.next empty
     * delete:empty flag=last flag;last flag=empty flag
     */
//    public static final int EMPTY = 0, USED = 1;
            public static final int USED=-1;

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

        maxRecord= (FileManager.BLOCK_SIZE-Flag_Size)/(layout.getRecordSize()+Flag_Size);
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

//    public void setEmpty(int slotNumber,boolean log){
//        transaction.setInt(blockId,calculateOffset(slotNumber),EMPTY,log);
//    }

    public void setEmpty(int slotNumber){
        int prev=transaction.getInt(blockId,FileManager.BLOCK_SIZE-Flag_Size);
        transaction.setInt(blockId,calculateOffset(slotNumber),prev,true);
        transaction.setInt(blockId,FileManager.BLOCK_SIZE-Flag_Size,slotNumber,true);
    }

//    public int setUsed(int slotNumber,boolean log){
//        transaction.setInt(blockId,calculateOffset(slotNumber),USED,log);
//    }

    public int setUsed(){
        int n=transaction.getInt(blockId,FileManager.BLOCK_SIZE-4);
        if(n==maxRecord){
            return -1;
        }
        int next=transaction.getInt(blockId,calculateOffset(n));
        transaction.setInt(blockId,calculateOffset(n),USED,true);
        transaction.setInt(blockId,FileManager.BLOCK_SIZE-Flag_Size,next,true);
        return n;
    }

    /**
     * 在当前block的slot number后找一个empty/used slot
     * @param slotNumber
     * @return empty slot number, 没找到返回-1
     */
    public int findUsedSlotAfter(int slotNumber){
        slotNumber++;
        while( slotNumber<maxRecord){
            int f=transaction.getInt(blockId,calculateOffset(slotNumber));
            if( f==USED){
                return slotNumber;
            }
            slotNumber++;
        }
        return -1;
    }

//    private boolean isFlag(int slotNumber,int flag){
//        int f=transaction.getInt(blockId,calculateOffset(slotNumber));
//        if(f!=0&&f!=1){
//            throw new DatabaseException("wrong flag in "+blockId+slotNumber);
//        }
//        return f==flag;
//    }

    public int getBlockNumber(){
        return blockId.getBlockNumber();
    }

    //没有null,format只改了flag位,insert/delete也没有format
//    public void format(){
//        int slotNumber=0;
//        while( slotNumber<maxRecord){
//            setEmpty(slotNumber,false);
//            slotNumber++;
//        }
//    }

    public void format(){
        int slotNumber=0;
        while(slotNumber<maxRecord){
            transaction.setInt(blockId,calculateOffset(slotNumber),slotNumber+1,false);
            slotNumber++;
        }
        transaction.setInt(blockId,FileManager.BLOCK_SIZE-4,0,false);
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
        return (FileManager.BLOCK_SIZE-Flag_Size)/ (Layout.countRecordSize(schema)+Flag_Size);
    }
}
