package transaction.concurrency;

import file.BlockId;

import java.lang.annotation.Retention;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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

    public synchronized void sLock(BlockId blockId){
        ReadWriteLock lock=getLock(blockId);
        synchronized (lock){
            try {
                long timeStamp=System.currentTimeMillis();
                while(lock.hasXLock() &&System.currentTimeMillis()-timeStamp<=MAX_WAIT_TIME){
                    lock.wait();
                }
                if(lock.hasXLock()){
                    throw new NoAvailableLockException();
                }
                lock.sLock();
            }catch (InterruptedException e){
                throw new NoAvailableLockException();
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
                while(lock.hasOtherLock()&&System.currentTimeMillis()-timeStamp<=MAX_WAIT_TIME){
                    lock.wait();
                }
                if(lock.hasOtherLock()){
                    throw new NoAvailableLockException();
                }
                lock.xLock();
            }catch (InterruptedException e){
                throw new NoAvailableLockException();
            }
        }
    }

    public synchronized void unLock(BlockId blockId){
        ReadWriteLock lock=getLock(blockId);
        synchronized (lock){
            if(lock.releaseALock()){
                lock.notifyAll();
            }
        }
    }

    private static class ReadWriteLock{

        int state=0;//-1表示x lock, 正数表示s lock的数量

        ReadWriteLock(){}

        boolean hasXLock(){
            return state<0;
        }

        boolean hasOtherLock(){
            return state!=0&&state!=1;
        }

        void sLock(){
            this.state++;
        }

        void xLock(){
            this.state=-1;
        }

        //这个lock上没有了其他的任何事务,需要notify
        boolean releaseALock(){
            if(hasXLock()){
                state=0;
                return true;
            }
            state--;
            return state==0;
        }
    }
}