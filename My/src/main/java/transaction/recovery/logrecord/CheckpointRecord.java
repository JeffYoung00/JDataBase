package transaction.recovery.logrecord;


import file.Page;
import lombok.ToString;

@ToString
public class CheckpointRecord implements LogRecord {

   private int[] runningTransactions;

   public CheckpointRecord(Page page) {
      int len=page.getInt(8);
      runningTransactions=new int[len];
      for(int i=0;i< runningTransactions.length;i++){
         runningTransactions[i]=page.getInt(12+4*i);
      }
   }

   public int type() {
      return CHECKPOINT;
   }

   public int transactionId() {
      return -1;//无意义
   }

   public static byte[] toLog(int[]runningTransactions){
      int len=12+4*runningTransactions.length;
      byte[]ret=new byte[len];
      Page page=new Page(ret);
      page.setInt(0, CHECKPOINT);
      page.setInt(4,-1);
      page.setInt(8, runningTransactions.length);
      for(int i=0;i< runningTransactions.length;i++){
         page.setInt(12+4*i,runningTransactions[i]);
      }
      return ret;
   }
}
