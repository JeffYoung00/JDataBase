package transaction.recovery.logrecord;

import file.BlockId;
import file.FileManager;
import file.Page;
import lombok.ToString;
import transaction.Transaction;

@ToString
public class SetIntRecord implements UpdateLogRecord {
   private int transactionId;
   private BlockId blockId;
   private int offset;
   private int oldValue;
   private int newValue;

   public SetIntRecord(Page page) {
      int index=4;

      transactionId= page.getInt(index);
      index+=4;

      String fileName=page.getString(index);
      index+=4;
      index+=fileName.getBytes(FileManager.FILE_CHARSET).length;

      int blockNumber=page.getInt(index);
      index+=4;

      this.blockId=new BlockId(fileName,blockNumber);

      this.offset=page.getInt(index);
      index+=4;

      this.oldValue=page.getInt(index);
      index+=4;

      this.newValue=page.getInt(index);
      index+=4;
   }

   public int type() {
      return SET_INT;
   }

   public int transactionId() {
      return transactionId;
   }

   public void undo(Transaction transaction) {
      transaction.setInt(blockId,offset,oldValue,false);
   }

   public void redo(Transaction transaction){
      transaction.setInt(blockId,offset,newValue,false);
   }

   public static byte[] toBytes(int transactionId, BlockId blockId,int offset,int oldValue,int newValue){
      int transactionIdIndex=4;
      int blockIdNameIndex=transactionIdIndex+ 4;
      int blockIdNumberIndex=blockIdNameIndex+ 4+blockId.getFileName().getBytes(FileManager.FILE_CHARSET).length;
      int offsetIndex=blockIdNumberIndex +4;
      int oldValueIndex=offsetIndex+ 4;
      int newValueIndex=oldValueIndex +4;
      int finalLen=newValueIndex+ 4;


      byte[]ret=new byte[finalLen];
      Page page=new Page(ret);
      page.setInt(0, SET_INT);
      page.setInt(transactionIdIndex,transactionId);
      page.setString(blockIdNameIndex, blockId.getFileName());
      page.setInt(blockIdNumberIndex, blockId.getBlockNumber());
      page.setInt(offsetIndex,offset);
      page.setInt(oldValueIndex,oldValue);
      page.setInt(newValueIndex,newValue);
      return ret;
   }
}
