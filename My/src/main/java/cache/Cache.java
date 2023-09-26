package cache;

import file.BlockId;
import file.FileManager;
import file.Page;
import log.LogManager;
import lombok.Getter;

import java.time.LocalDateTime;
import java.util.Calendar;
import java.util.Date;

public class Cache {

    private FileManager fileManager;
    private LogManager logManager;


    @Getter private Page content;
    @Getter private BlockId blockId=null;

    @Getter private LocalDateTime startDate;
    @Getter private int totalPins;
    private int pins;

    //为什么需要记录modify transaction id->rollback/commit时需要遍历刷新cache到disk,如果是undo+redo就不需要
    //private int modifyTransactionId=-1;

    private boolean modified=false;
    private int logNumber=-1;

    public Cache(FileManager fileManager, LogManager logManager){
        newState();

        this.fileManager=fileManager;
        this.logManager=logManager;
        this.content=new Page(new byte[FileManager.BLOCK_SIZE]);
    }

    public void modifyLogNumber(int logNumber){
        this.modified=true;
        this.logNumber=logNumber;
    }

    /**
     * 只记录被修改,恢复的内容不需要记录log
     */
    public void modify(){
        modified=true;
    }

    public boolean isModified(){
        return modified;
    }

    public void flush(){
        if(logNumber!=-1){
            logManager.flush(logNumber);
        }
        logNumber=-1;
        if(modified){
            fileManager.write(blockId,content);
        }
        modified=false;
    }

    public void fillUp(BlockId blockId){
        //先将内容flush
        flush();
        newState();
        this.blockId=blockId;
        fileManager.read(blockId,content);
    }

    public boolean isPinned(){
        return pins>0;
    }

    public void pin(){
        pins++;
        totalPins++;
    }

    public void unpin(){
        pins--;
    }

    private void newState(){
        totalPins=0;
        pins=0;
        startDate= LocalDateTime.now();

        //modified不应该为false
        //modified=false;
        //logNumber=-1;
    }

}
