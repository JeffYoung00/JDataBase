package transaction.concurrency;

import cache.Cache;
import file.BlockId;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;


public class ReadUncommittedConcurrencyManager implements Concurrency{
    private int transactionId;
    private Set<BlockId> xLocks=new HashSet<>();

    public ReadUncommittedConcurrencyManager(int transactionId){
        this.transactionId=transactionId;
    }

    @Override
    public void sLock(BlockId blockId) {
        //do nothing
    }

    //不需要先申请s锁
    @Override
    public void xLock(BlockId blockId) {
        if(xLocks.contains(blockId)){
            return;
        }
        Concurrency.lockTable.xLock(blockId,transactionId);
        xLocks.add(blockId);
    }

    @Override
    public void releaseAll() {
        for(BlockId blockId:xLocks){
            Concurrency.lockTable.unLock(blockId,transactionId);
        }
    }
}
