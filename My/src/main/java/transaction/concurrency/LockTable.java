package transaction.concurrency;

import file.BlockId;

import java.util.*;

public class LockTable {

    private Map<BlockId,ReadWriteLock> lockMap=new HashMap<>();

    private static final long MAX_WAIT_TIME=10000;

    private ReadWriteLock getLock(BlockId blockId){
        ReadWriteLock ret=lockMap.get(blockId);
        if(ret==null){
            ret=new ReadWriteLock();
            lockMap.put(blockId,ret);
        }
        return ret;
    }

    public synchronized ArrayList<Integer> getTxs(BlockId blockId){
        ReadWriteLock lock=lockMap.get(blockId);
        return lock==null?null:lock.getTxs();
    }

    public synchronized void sLock(BlockId blockId,int transactionId){
        ReadWriteLock lock=getLock(blockId);
        synchronized (lock){
            try {
                long timeStamp=System.currentTimeMillis();
                if(lock.hasXLock()){
                    if(!Concurrency.waitForGraph.put(transactionId,blockId)){
                        throw new DeadLockException();
                    }
                    lock.wait();
                }
                while(lock.hasXLock() &&System.currentTimeMillis()-timeStamp<=MAX_WAIT_TIME){
                    lock.wait();
                }
                if(lock.hasXLock()){
                    throw new DeadLockException();
                }
                lock.sLock(transactionId);
                Concurrency.waitForGraph.remove(transactionId);
            }catch (InterruptedException e){
                throw new DeadLockException();
            }
        }
    }

    //上面有一个s锁会被认为是自己申请的,需要concurrency先申请s锁
    //read uncommitted没有s锁
    public synchronized void xLock(BlockId blockId, int transactionId){
        ReadWriteLock lock=getLock(blockId);
        synchronized (lock){
            try {
                long timeStamp=System.currentTimeMillis();
                if(lock.hasOtherLock()){
                    if(!Concurrency.waitForGraph.put(transactionId,blockId)){
                        throw new DeadLockException();
                    }
                    lock.wait();
                }
                while(lock.hasOtherLock()&&System.currentTimeMillis()-timeStamp<=MAX_WAIT_TIME){
                    lock.wait();
                }
                if(lock.hasOtherLock()){
                    throw new DeadLockException();
                }
                lock.xLock(transactionId);
                Concurrency.waitForGraph.remove(transactionId);
            }catch (InterruptedException e){
                throw new DeadLockException();
            }
        }
    }

    public synchronized void unLock(BlockId blockId,int transactionId){
        ReadWriteLock lock=getLock(blockId);
        synchronized (lock){
            if(lock.releaseALock(transactionId)){
                lock.notifyAll();
            }
        }
    }

    private static class ReadWriteLock{

        int state=0;//-1表示x lock, 正数表示s lock的数量

        ArrayList<Integer> txs;

        ReadWriteLock(){txs=new ArrayList<>();}

        ArrayList<Integer> getTxs(){
            return new ArrayList<>(txs);
        }

        boolean hasXLock(){
            return state<0;
        }

        boolean hasOtherLock(){
            return state!=0&&state!=1;
        }

        void sLock(int tx){
            this.state++;
            txs.add(tx);
        }

        void xLock(int tx){
            this.state=-1;
            txs.add(tx);
        }

        //这个lock上没有了其他的任何事务,需要notify
        boolean releaseALock(int tx){
            if(hasXLock()){
                state=0;
                return true;
            }
            state--;
            txs.remove(tx);
            return state==0;
        }
    }
}