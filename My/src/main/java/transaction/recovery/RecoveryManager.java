package transaction.recovery;

import cache.CacheManager;
import file.FileManager;
import file.Page;
import log.LogManager;
import server.Database;
import server.DatabaseException;
import transaction.Transaction;
import transaction.recovery.logrecord.*;

import java.util.*;
import java.util.stream.Collectors;

public class RecoveryManager {
    private CacheManager cacheManager;
    private LogManager logManager;
    private FileManager fileManager;

    public RecoveryManager(FileManager fileManager, LogManager logManager, CacheManager cacheManager){
        this.fileManager=fileManager;
        this.logManager=logManager;
        this.cacheManager=cacheManager;
    }

    public void dynamicCheckPoint(){
        //不再产生新的transaction
        synchronized (Transaction.class){
            /**
             * 如果transaction刚刚开始,那么他会等待Transaction.class,不会有影响
             * 如果transaction正在运行中,那么会竞争cacheManager,可能导致将更新到一半的内容写回磁盘,但不会有cache交换,问题不大
             * 如果transaction运行结束,那么它会等待Transaction.class,不会有影响
             */
            cacheManager.flushAll();
            logManager.newLogRecord(CheckpointRecord.toLog(Transaction.getRunningTransactions()));
        }
    }

    /**
     * 每次开机会执行这个函数,可以在意外shutdown之后恢复,或者正常shutdown之后正常运行
     */
    public void doRecover(Transaction systemTransaction){

        Iterator<byte[]>iterator= logManager.iterator();

        List<UpdateLogRecord>logList=new ArrayList<>();

        List<Integer> newCommittedTransactions=new ArrayList<>();
        List<Integer> runningTransactions=new ArrayList<>();
        List<Integer> startTransactions=new ArrayList<>();

        Page page=new Page(null);

        //找到上一条checkpoint, 即所有还没有执行结束的所有事务
        //并记录需要恢复前所有的commit事务,rollback事务
        while(iterator.hasNext()){
            byte[] log= iterator.next();
            page.setContent(log);
            int type=page.getInt(0);
            if(type== LogRecord.CHECKPOINT){
                //解析checkpoint
                int len=page.getInt(8);
                for(int i=0;i<len;i++){
                    runningTransactions.add(page.getInt(12+4*i));
                }
                break;
            } else if (type==LogRecord.COMMIT) {
                newCommittedTransactions.add(page.getInt(4));
            }else if(type==LogRecord.ROLLBACK){
//                newRollbackTransactions.add(page.getInt(4));
            }else if(type==LogRecord.SET_INT){
                logList.add(new SetIntRecord(page));
            }else if(type==LogRecord.SET_STRING){
                logList.add(new SetStringRecord(page));
            }else if(type==LogRecord.START){
                startTransactions.add(page.getInt(4));
            }
        }

        //redo: commit list
        //undo: start+running-commit
        Set<Integer> redoSet=new HashSet<>(newCommittedTransactions);
        Set<Integer> undoSet=new HashSet<>();
        for(Integer i:startTransactions){
            if(!redoSet.contains(i)){
                undoSet.add(i);
            }
        }
        for(Integer i:runningTransactions){
            if(!redoSet.contains(i)){
                undoSet.add(i);
            }
        }

        //第一次启动/正常关机
        if(redoSet.isEmpty()&&undoSet.isEmpty()){
            return;
        }

        //遍历到checkpoint中的第一个start
        while(!runningTransactions.isEmpty()){
            if(!iterator.hasNext()){
                throw new DatabaseException("not found all start logs of running transactions");
            }
            byte[] log= iterator.next();
            page.setContent(log);
            int type=page.getInt(0);
            if(type==LogRecord.START){
                int transactionId=page.getInt(4);
                runningTransactions.remove(transactionId);
            }else if(type==LogRecord.SET_INT){
                logList.add(new SetIntRecord(page));
            }else if(type==LogRecord.SET_STRING){
                logList.add(new SetStringRecord(page));
            }
        }

        startRecover(systemTransaction,logList,undoSet,redoSet);

        //flush all恢复正确状态,因为下面有emptyFile
        cacheManager.flushAll();

        //直接刷新log file内容
        logManager.emptyLog();
    }

    //loglist本身是自后向前的
    public void startRecover(Transaction systemTransaction,List<UpdateLogRecord>logList,Set<Integer>undoTransactions,
                             Set<Integer>redoTransactions){
        //逆序undo
        for(UpdateLogRecord logRecord:logList){
            if (undoTransactions.contains(logRecord.transactionId())){
                logRecord.undo(systemTransaction);
            }
        }

        //顺序redo
        for(int i=logList.size()-1;i>=0;i--){
            UpdateLogRecord logRecord=logList.get(i);
            if(redoTransactions.contains(logRecord.transactionId())){
                logRecord.redo(systemTransaction);
            }
        }
    }
}
