package log;

import file.BlockId;
import file.FileManager;
import file.Page;
import server.Database;

import java.util.Iterator;

public class LogIterator implements Iterator<byte[]> {

    BlockId currentBlockId;
    FileManager fileManager;
    Page page;
    int currentPosition;

    LogIterator(FileManager fileManager, BlockId blockId){
        this.currentBlockId =blockId;
        this.fileManager=fileManager;
        this.page=new Page(new byte[FileManager.BLOCK_SIZE]);
        fileManager.read(currentBlockId,page);
        currentPosition=page.getInt(0);
    }

    /**
     * 从每个block的boundary开始读到块的末尾,然后移到上一个块,直到移动到第0个块的末尾
     * 遍历的结果是 时间上的 倒序
     */
    @Override
    public boolean hasNext() {
        return currentPosition< FileManager.BLOCK_SIZE || currentBlockId.getBlockNumber()>0;
    }

    @Override
    public byte[] next() {
        while(currentPosition==FileManager.BLOCK_SIZE){
            moveToPreBlock();
        }
        byte[] ret= page.getBytes(currentPosition);
        currentPosition+=ret.length+4;
        return ret;
    }

    private void moveToPreBlock(){
        currentBlockId.setBlockNumber(currentBlockId.getBlockNumber()-1 );
        fileManager.read(currentBlockId,page);
        currentPosition=page.getInt(0);
    }
}
