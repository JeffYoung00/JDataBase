package transaction;

import cache.Cache;
import cache.CacheManager;
import file.BlockId;
import file.FileManager;
import file.Page;
import log.LogManager;
import server.Database;
import server.DatabaseException;
import transaction.concurrency.Concurrency;
import transaction.concurrency.ReadCommittedConcurrencyManager;
import transaction.concurrency.ReadRepeatableConcurrencyManager;
import transaction.concurrency.ReadUncommittedConcurrencyManager;
import transaction.recovery.logrecord.*;

import java.sql.Connection;
import java.util.*;

public class Transaction {

    private static int TransactionId=-1;
    private static HashSet<Integer> runningTransactions=new HashSet<>();

    public synchronized static int getNextTransactionId(){
        TransactionId++;
        runningTransactions.add(TransactionId);
        return TransactionId;
    }

    public synchronized static void removeTransactionId(int transactionId){
        runningTransactions.remove(transactionId);
    }

    public synchronized static int[] getRunningTransactions(){
        return runningTransactions.stream().mapToInt(Integer::intValue).toArray();
    }


    /**
     * 标志着 文件结束 的块号
     */
    private static final int END_OF_FILE=-1;


    private int transactionId;
    private boolean finished=false;

    private Concurrency concurrency;
    private CacheManager cacheManager;
    private LogManager logManager;
    private FileManager fileManager;

    private Map<BlockId, Cache> pinnedCaches=new HashMap<>();


    public Transaction(FileManager fileManager, CacheManager cacheManager, LogManager logManager){
        this.transactionId=getNextTransactionId();

        if(Database.Isolation_Level== Connection.TRANSACTION_READ_UNCOMMITTED){
            this.concurrency =new ReadUncommittedConcurrencyManager(transactionId);
        } else if (Database.Isolation_Level==Connection.TRANSACTION_READ_COMMITTED) {
            this.concurrency =new ReadCommittedConcurrencyManager(transactionId,pinnedCaches);
        }else if(Database.Isolation_Level==Connection.TRANSACTION_REPEATABLE_READ){
            this.concurrency =new ReadRepeatableConcurrencyManager(transactionId);
        }else{
            throw new DatabaseException("unsupported isolation level");
        }

        this.fileManager=fileManager;
        this.cacheManager=cacheManager;
        this.logManager=logManager;

        //start record
        logManager.newLogRecord(StartRecord.toLog(transactionId));
    }


    public Cache pin(BlockId blockId){
        Cache cache=pinnedCaches.get(blockId);
        if(cache==null){
            cache=cacheManager.pin(blockId);
            pinnedCaches.put(blockId,cache);
        }
        return cache;
    }

    public void unpin(BlockId blockId){
        Cache cache=pinnedCaches.remove(blockId);
        cacheManager.unpin(cache);
    }

    //正常释放资源
    void release(){
        for(Cache cache:pinnedCaches.values()){
            cache.unpin();
        }
        concurrency.releaseAll();
    }

    public void commit(){
        if(finished){
            return;
        }
        release();
        removeTransactionId(transactionId);
        int i = logManager.newLogRecord(CommitRecord.toLog(transactionId));
        logManager.flush(i);
        finished=true;
    }

    public void rollback(){
        if(finished){
            return;
        }
        Iterator<byte[]> iterator=logManager.iterator();
        Page page=new Page(null);
        while(iterator.hasNext()){
            byte[] recordBytes=iterator.next();
            page.setContent(recordBytes);
            if(page.getInt(4)==transactionId){
                int type=page.getInt(0);
                if(type== LogRecord.START){
                    break;
                }else if(type==LogRecord.SET_INT){
                    SetIntRecord r=new SetIntRecord(page);
                    r.undo(this);
                }else if(type==LogRecord.SET_STRING){
                    SetStringRecord r=new SetStringRecord(page);
                    r.undo(this);
                }
            }
        }
        release();
        removeTransactionId(transactionId);
        int i = logManager.newLogRecord(RollbackRecord.toLog(transactionId));
        logManager.flush(i);
        finished=true;
    }

    public int getInt(BlockId blockId, int offset){
        concurrency.sLock(blockId);
        Cache cache=pin(blockId);
        Page page = cache.getContent();
        return page.getInt(offset);
    }

    public String getString(BlockId blockId, int offset){
        concurrency.sLock(blockId);
        Cache cache=pin(blockId);
        Page page = cache.getContent();
        return page.getString(offset);
    }


    public void setString(BlockId blockId, int offset, String value,boolean writeLog){
        concurrency.xLock(blockId);
        Cache cache=pin(blockId);
        Page page=cache.getContent();
        String oldValue=page.getString(offset);
        page.setString(offset,value);

        byte[]record= SetStringRecord.toBytes(transactionId,blockId,offset,oldValue,value);
        int logNumber = logManager.newLogRecord(record);
        if(writeLog){
            cache.modifyLogNumber(logNumber);
        }else{
            cache.modify();
        }
    }

    public void setInt(BlockId blockId, int offset, int value,boolean writeLog) {
        concurrency.xLock(blockId);
        Cache cache=pin(blockId);
        Page page=cache.getContent();
        int oldValue=page.getInt(offset);
        page.setInt(offset,value);

        byte[]record= SetIntRecord.toBytes(transactionId,blockId,offset,oldValue,value);
        int logNumber = logManager.newLogRecord(record);
        if(writeLog){
            cache.modifyLogNumber(logNumber);
        }else{
            cache.modify();
        }
    }

    /**
     * size和new block
     */
    public int fileBlockLen(String fileName){
        BlockId dummyBlock=new BlockId(fileName,END_OF_FILE);
        concurrency.sLock(dummyBlock);
        return fileManager.fileBlockLen(fileName);
    }

    public BlockId appendNewFileBlock(String fileName){
        BlockId dummyBlock=new BlockId(fileName,END_OF_FILE);
        concurrency.sLock(dummyBlock);
        concurrency.xLock(dummyBlock);
        return fileManager.appendNewFileBlock(fileName);
    }

    public void emptyTempFile(String fileName){
        if(!fileName.endsWith(Database.tempTablePostfix)){
            throw new DatabaseException("try to empty a non-temp table");
        }
        fileManager.emptyFile(fileName);
    }


    //用于temp table
    //默认不write to log
    public void getOriginBytes(BlockId blockId,int offset,byte[]bytes){
        Cache cache=pinnedCaches.get(blockId);
        Page page=cache.getContent();
        page.getOriginBytes(offset,bytes);
    }

    public void setOriginBytes(BlockId blockId,int offset,byte[]bytes){
        concurrency.xLock(blockId);
        Cache cache=pinnedCaches.get(blockId);
        Page page=cache.getContent();
        int oldValue=page.getInt(offset);
        page.setOriginBytes(offset,bytes);
    }

    /**
     * 为了解决b+树的死锁问题
     */
    private ArrayList<BlockId> bPlusXLock=new ArrayList<>();
    public void xLockForBPlusTree(BlockId blockId){
        bPlusXLock.add(blockId);
        concurrency.xLock(blockId);
    }
    public void releaseXLockForBPlusTree(){
        for(BlockId b:bPlusXLock){
            Concurrency.lockTable.unLock(b,transactionId);
            Concurrency.lockTable.sLock(b,transactionId);
        }
    }
}

