package log;

import file.BlockId;
import file.FileManager;
import file.Page;
import lombok.Getter;
import server.Database;
import server.DatabaseException;

import java.util.Iterator;

/**
 * log block 格式
 * xxxx(boundary)-- space-- len+content | len+content...|end
 * 新log在前
 */
public class LogManager {
    private FileManager fileManager;
    private Page logPage;
    private BlockId logBlock;

    private int newLogNumber=0;
    private int lastSavedLogNumber=0;

    public LogManager(FileManager fileManager){
        this.fileManager=fileManager;
        int logLen=fileManager.fileBlockLen(Database.logFileName);
        if(logLen==0){
            logBlock=fileManager.appendNewFileBlock(Database.logFileName);
            logPage=new Page(new byte[FileManager.BLOCK_SIZE]);
            //!!初始化
            //!!初始化之后也要写回 ,直接加上format()
            format();

        }else{
            logBlock=new BlockId(Database.logFileName,logLen-1);
            logPage=new Page(new byte[FileManager.BLOCK_SIZE]);
            fileManager.read(logBlock,logPage);
        }
    }

    private void format(){
        logPage.setInt(0,FileManager.BLOCK_SIZE);
        fileManager.write(logBlock,logPage);
    }

    public synchronized int newLogRecord(byte[] logRecord){
        int boundary=logPage.getInt(0);
        if(boundary<4){
            throw new DatabaseException("error boundary in "+logBlock.getBlockNumber());
        }
        //如果当前page装不下,需要更新page
        if(boundary-4<logRecord.length+4){

            //!!
//            fileManager.write(logBlock,logPage);
            flushAll();

            appendNewLogBlock();
            //!!更新boundary
            boundary=FileManager.BLOCK_SIZE;
        }
        logPage.setBytes(boundary-4-logRecord.length,logRecord);
        newLogNumber++;
        //!!更新boundary
        logPage.setInt(0,boundary-4-logRecord.length);
        return newLogNumber;
    }

    private void appendNewLogBlock(){
        // 这里不new Page,循环利用
        //logPage=new Page();
        logBlock=fileManager.appendNewFileBlock(logBlock.getFileName());

        //!! 因为logpage的格式,如果不写回而异常中断则问题严重
        format();
    }

    public synchronized void flushAll(){
        fileManager.write(logBlock,logPage);
        lastSavedLogNumber=newLogNumber;
    }

    //这个方法和lastSavedLogNumber/newLogNumber字段是为了减少写log磁盘io的频率
    public synchronized void flush(int saveLogNumber){
        if(saveLogNumber>lastSavedLogNumber){
            fileManager.write(logBlock,logPage);
            lastSavedLogNumber=saveLogNumber;
        }
    }

    public synchronized Iterator<byte[]> iterator(){
        //!! 使用文件内容迭代,先要刷新到文件让两边内容保持一致
        flushAll();
        return new LogIterator(fileManager,logBlock);
    }

    public synchronized void emptyLog(){
        logPage.setInt(0,FileManager.BLOCK_SIZE);
        fileManager.emptyFile(Database.logFileName);
    }
}
