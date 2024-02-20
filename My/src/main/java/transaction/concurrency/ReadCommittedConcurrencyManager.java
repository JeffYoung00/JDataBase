package transaction.concurrency;

import cache.Cache;
import file.BlockId;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

public class ReadCommittedConcurrencyManager implements Concurrency{

    public static int Lock_Factor=2;

    private int transactionId;
    private Map<BlockId, Cache> pinnedCaches;

    //因为删除slock时,不应该删除有xlock的slock,这没有意义
    // 所以将slock和xlock分开,xlock虽然会先申请slock,但并不会保存在slock中
    private Set<BlockId> sLocks=new LinkedHashSet<>();
    private Set<BlockId> xLocks=new HashSet<>();


    public ReadCommittedConcurrencyManager(int transactionId, Map<BlockId, Cache> pinnedCaches){
        this.transactionId=transactionId;
        this.pinnedCaches=pinnedCaches;
    }

    @Override
    public void sLock(BlockId blockId) {
        int release=sLocks.size()-pinnedCaches.size()*Lock_Factor;
        for(int i=0;i<release;i++){
            BlockId first = sLocks.iterator().next();
            sLocks.remove(first);
        }

        if(xLocks.contains(blockId)){
            return;
        }
        if(sLocks.contains(blockId)){
            sLocks.remove(blockId);
            sLocks.add(blockId);
            return;
        }
        Concurrency.lockTable.sLock(blockId,transactionId);
        sLocks.add(blockId);
    }

    @Override
    public void xLock(BlockId blockId) {
        if(xLocks.contains(blockId)){
            return;
        }
        if(!sLocks.contains(blockId)){
            Concurrency.lockTable.sLock(blockId,transactionId);
        }
        Concurrency.lockTable.xLock(blockId,transactionId);
        xLocks.add(blockId);
    }

    @Override
    public void releaseAll() {
        for(BlockId blockId:xLocks){
            Concurrency.lockTable.unLock(blockId,transactionId);
        }
        for(BlockId blockId:sLocks){
            Concurrency.lockTable.unLock(blockId,transactionId);
        }
    }
}
