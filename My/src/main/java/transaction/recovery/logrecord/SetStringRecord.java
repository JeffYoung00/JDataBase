package transaction.recovery.logrecord;

import file.BlockId;
import file.FileManager;
import file.Page;
import lombok.ToString;
import transaction.Transaction;

@ToString
public class SetStringRecord implements UpdateLogRecord {
   private int transactionId;
   private BlockId blockId;
   private int offset;
   private String oldValue;
   private String newValue;

   public SetStringRecord(Page page) {
      int index=4;

      transactionId=page.getInt(index);
      index+=4;

      String fileName=page.getString(index);
      index+=4;
      index+=fileName.getBytes(FileManager.FILE_CHARSET).length;

      int blockNumber=page.getInt(index);
      index+=4;

      this.blockId=new BlockId(fileName,blockNumber);

      this.offset=page.getInt(index);
      index+=4;

      this.oldValue=page.getString(index);
      index+=4;
      index+=oldValue.getBytes(FileManager.FILE_CHARSET).length;

      this.newValue=page.getString(index);
      index+=4;
      index+=newValue.getBytes(FileManager.FILE_CHARSET).length;
   }

   public int type() {
      return SET_STRING;
   }

   public int transactionId() {
      return transactionId;
   }

   public void undo(Transaction transaction) {
      transaction.setString(blockId,offset,oldValue,false);
   }

   public void redo(Transaction transaction){
      transaction.setString(blockId,offset,newValue,false);
   }

   public static byte[] toBytes(int transactionId, BlockId blockId,int offset,String oldValue,String newValue){
      int transactionIdIndex=4;
      int blockIdNameIndex=transactionIdIndex+ 4;
      int blockIdNumberIndex=blockIdNameIndex+ 4+blockId.getFileName().getBytes(FileManager.FILE_CHARSET).length;
      int offsetIndex=blockIdNumberIndex +4;
      int oldValueIndex=offsetIndex+ 4;
      int newValueIndex=oldValueIndex +4+oldValue.getBytes(FileManager.FILE_CHARSET).length;
      int finalLen=newValueIndex+ 4+newValue.getBytes(FileManager.FILE_CHARSET).length;

      byte[]ret=new byte[finalLen];
      Page page=new Page(ret);
      page.setInt(0,SET_STRING);
      page.setInt(transactionIdIndex,transactionId);
      page.setString(blockIdNameIndex,blockId.getFileName());
      page.setInt(blockIdNumberIndex,blockId.getBlockNumber());
      page.setInt(offsetIndex,offset);
      page.setString(oldValueIndex,oldValue);
      page.setString(newValueIndex,newValue);
      return ret;
   }
}
