package transaction.concurrency;

import file.BlockId;

public interface Concurrency {

    LockTable lockTable=new LockTable();

    WaitForGraph waitForGraph=new WaitForGraph();

    void sLock(BlockId blockId);
    void xLock(BlockId blockId);
    void releaseAll();
}
