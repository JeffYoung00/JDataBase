package file;

import lombok.Getter;
import server.DatabaseException;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * 负责文件的创建,文件的块级读写,文件的块级拓展
 */
public class FileManager {
    /**
     * 一个Block对应的字节数
     */
    public static  int BLOCK_SIZE=512;
    /**
     * 文件存储的字符集
     */
    public static Charset FILE_CHARSET= StandardCharsets.UTF_16;

    private String databaseDirName;

    //filename to file
    Map<String, RandomAccessFile> fileMap=new HashMap<>();

    public static void init(String databaseDirName){
        File databaseDir=new File(databaseDirName);
        if(!databaseDir.exists()){
            databaseDir.mkdir();
        }
    }

    public FileManager(String databaseDirName){
        this.databaseDirName=databaseDirName;
    }

    public void read(BlockId from, Page page){
        try{
            RandomAccessFile file=getFile(from.getFileName());
            file.seek((long) BLOCK_SIZE *from.getBlockNumber());
            int ret=file.read(page.getContent());
            if(-1==ret){
                throw new BadReadingException("Read: "+from);
            }
        }catch (IOException e){
            throw new BadReadingException("Read: "+from);
        }
    }

    public void write(BlockId to, Page page){
        try{
            RandomAccessFile file=getFile(to.getFileName());
            file.seek((long) BLOCK_SIZE *to.getBlockNumber());
            file.write(page.getContent());
        }catch (IOException e){
            throw new BadReadingException("Write: "+to);
        }
    }

    public int fileBlockLen(String fileName){
        try{
            RandomAccessFile file=getFile(fileName);
            int len=(int)file.length();
            if(len%FileManager.BLOCK_SIZE!=0){
                throw new DatabaseException("file block len");
            }
            return len/FileManager.BLOCK_SIZE;
        }catch (IOException e){
            throw new BadReadingException("Len: "+fileName);
        }
    }


    public BlockId appendNewFileBlock(String fileName){
        try{
            int blockLen= fileBlockLen(fileName);
            RandomAccessFile file=getFile(fileName);
            byte[]empty=new byte[FileManager.BLOCK_SIZE];
            file.seek((long)blockLen*FileManager.BLOCK_SIZE);
            file.write(empty);
            return new BlockId(fileName,blockLen);
        }catch (IOException e){
            throw new BadReadingException("New: "+fileName);
        }

    }

    private RandomAccessFile getFile(String fileName)throws IOException{
        RandomAccessFile randomAccessFile=fileMap.get(fileName);
        if(randomAccessFile==null){
            File file=new File(databaseDirName,fileName);
            //不存在会创建
            randomAccessFile=new RandomAccessFile(file,"rws");
            fileMap.put(fileName,randomAccessFile);
        }
        return randomAccessFile;
    }

    public void emptyFile(String fileName){
        try{
            RandomAccessFile randomAccessFile=getFile(fileName);
            randomAccessFile.seek(0);
            randomAccessFile.setLength(0);
        }catch (IOException e){
            e.printStackTrace();
        }
    }
}
