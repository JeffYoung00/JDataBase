package transaction.recovery.logrecord;


import file.Page;
import lombok.ToString;

@ToString
public class StartRecord implements LogRecord {
   private int transactionId;

   public StartRecord() {
   }

   public int type() {
      return START;
   }

   public int transactionId() {
      return transactionId;
   }

   public static byte[] toLog(int transactionId){
      byte[]ret=new byte[8];
      Page page=new Page(ret);
      page.setInt(0, START);
      page.setInt(4,transactionId);
      return ret;
   }
}
