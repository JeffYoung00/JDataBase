package transaction.concurrency;

import file.BlockId;

public interface Concurrency {

    LockTable lockTable=new LockTable();

    void sLock(BlockId blockId);
    void xLock(BlockId blockId);
    void releaseAll();
}
