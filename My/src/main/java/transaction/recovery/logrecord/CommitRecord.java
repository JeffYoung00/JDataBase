package transaction.recovery.logrecord;


import file.Page;
import lombok.ToString;

@ToString
public class CommitRecord implements LogRecord {

   private int transactionId;

   public CommitRecord() {
   }

   public int type() {
      return COMMIT;
   }

   public int transactionId() {
      return transactionId;
   }

   public static byte[] toLog(int transactionId){
      byte[]ret=new byte[8];
      Page page=new Page(ret);
      page.setInt(0, COMMIT);
      page.setInt(4,transactionId);
      return ret;
   }
}
