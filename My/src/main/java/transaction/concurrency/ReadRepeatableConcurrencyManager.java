package transaction.concurrency;

import file.BlockId;

import java.util.*;

public class ReadRepeatableConcurrencyManager implements Concurrency{

    private final int transactionId;

    //用false表示读锁,用true表示写锁
    private Map<BlockId,Boolean> transactionLockTable=new HashMap<>();

    public ReadRepeatableConcurrencyManager(int transactionId){
        this.transactionId=transactionId;
    }

    public void sLock(BlockId blockId){
        Boolean lock=transactionLockTable.get(blockId);
        if(lock==null){
            Concurrency.lockTable.sLock(blockId,transactionId);
            transactionLockTable.put(blockId,false);
        }
    }

    public void xLock(BlockId blockId){
        Boolean lock=transactionLockTable.get(blockId);
        if(lock==null|| !lock){
            //先申请s锁
            sLock(blockId);

            Concurrency.lockTable.xLock(blockId,transactionId);
            transactionLockTable.put(blockId,true);
        }
    }

    public void releaseAll(){
        for(BlockId blockId:transactionLockTable.keySet()){
            Concurrency.lockTable.unLock(blockId,transactionId);
        }
    }
}
