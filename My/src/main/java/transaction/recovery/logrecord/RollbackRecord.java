package transaction.recovery.logrecord;


import file.Page;
import lombok.ToString;

@ToString
public class RollbackRecord implements LogRecord {
   private int transactionId;

   public RollbackRecord() {
   }

   public int type() {
      return ROLLBACK;
   }

   public int transactionId() {
      return transactionId;
   }

   public static byte[] toLog(int transactionId){
      byte[]ret=new byte[8];
      Page page=new Page(ret);
      page.setInt(0, ROLLBACK);
      page.setInt(4,transactionId);
      return ret;
   }
}
