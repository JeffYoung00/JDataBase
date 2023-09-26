package file;


import server.DatabaseException;

import java.util.Arrays;

/**
 * 处理byte[]读写的工具类
 */
public class Page {
    private byte[] content;

    public Page(byte[] content){
        this.content=content;
    }

    public int getInt(int offset){
        if(offset+4>FileManager.BLOCK_SIZE||offset<0){
            throw new DatabaseException("page cross this line");
        }
        int value=0;
        for(int i=0;i<4;i++){
            value= value<<8 | (content[offset+i]&0xFF);
        }
        return value;
    }

    public void setInt(int offset,int value){
        if(offset+4>FileManager.BLOCK_SIZE||offset<0){
            throw new DatabaseException("page cross this line");
        }
        for(int i=0;i<4;i++){
            content[offset+i]= (byte) (value>>(24-8*i)&0xFF);
        }
    }

    public byte[] getBytes(int offset){
        int len=getInt(offset);
        if(offset+4+len>FileManager.BLOCK_SIZE||offset<0||len<0){
            throw new DatabaseException("page cross this line, len:"+len+" offset:"+offset);
        }
        return Arrays.copyOfRange(content, offset + 4, offset + 4 + len);
    }

    public void setBytes(int offset,byte[]value){
        if(offset+4+value.length>FileManager.BLOCK_SIZE||offset<0){
            throw new DatabaseException("page cross this line");
        }
        setInt(offset,value.length);
        for(int i=0;i<value.length;i++){
            content[offset+4+i]=value[i];
        }
    }

    public void getOriginBytes(int offset,byte[] bytes){
        if(offset+bytes.length>FileManager.BLOCK_SIZE||offset<0){
            throw new DatabaseException("page cross this line");
        }
        System.arraycopy(content,offset,bytes,0,bytes.length);
    }

    public void setOriginBytes(int offset,byte[] bytes){
        if(offset+bytes.length>FileManager.BLOCK_SIZE||offset<0){
            throw new DatabaseException("page cross this line");
        }
        System.arraycopy(bytes,0,content,offset,bytes.length);
    }

    public String getString(int offset){
        byte[] stringBytes=getBytes(offset);
        return new String(stringBytes,FileManager.FILE_CHARSET);
    }
    
    public void setString(int offset,String value){
        byte[] stringBytes=value.getBytes(FileManager.FILE_CHARSET);
        setBytes(offset,stringBytes);
    }

    public byte[] getContent(){
        return content;
    }

    public void setContent(byte[]content){
        this.content=content;
    }
}
