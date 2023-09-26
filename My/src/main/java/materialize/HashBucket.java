package materialize;

import lombok.Getter;
import record.Layout;
import record.RecordPage;
import record.Schema;
import record.TableScan;
import scan.Scan;
import transaction.Transaction;

import java.util.*;

public class HashBucket {

    Transaction transaction;

    String leftHashFieldName;
    String rightHashFieldName;
    Scan leftScan;
    Scan rightScan;
    Layout leftLayout;
    Layout rightLayout;

    int hashBits;
    int maxHash;
    int expectedBucketLen;

    @Getter List<TemporaryTable> lastTemporaryTablesLeft=new ArrayList<>();
    @Getter List<TemporaryTable> lastTemporaryTablesRight=new ArrayList<>();

    //hashBits 1次hash多少bit,需要2^hashBits个buffer
    public HashBucket(Transaction transaction, Schema leftSchema, Schema rightSchema,
                      String leftHashFieldName, String rightHashFieldName,Scan leftScan, Scan rightScan,
                      int hashBits, int maxHash){
        this.transaction=transaction;
        this.leftHashFieldName = leftHashFieldName;
        this.rightHashFieldName=rightHashFieldName;
        this.leftScan=leftScan;
        this.rightScan=rightScan;
        this.hashBits=hashBits;
        this.maxHash=maxHash;

        //expected
        this.expectedBucketLen=1<<hashBits<<1;
        this.leftLayout=new Layout(leftSchema);
        this.rightLayout=new Layout(rightSchema);

        start();
    }

    /**
     * 每次分桶,记录size,根据size决定 继续分桶/丢弃/加入结果集
     * 判断标准是expected bucket len,即理想hash均分的两倍
     */
    void start(){

        Queue<TemporaryTable> tempLeftQueue=new ArrayDeque<>();
        Queue<TemporaryTable> tempRightQueue=new ArrayDeque<>();

        for(int count=0;count<maxHash;count++){
            Scan currentLeftScan;
            Scan currentRightScan;
            TemporaryTable leftTemp=null;
            TemporaryTable rightTemp=null;
            if(count==0){
                currentLeftScan=leftScan;
                currentRightScan=rightScan;
            }else if(tempLeftQueue.isEmpty()){
                break;
            }else {
                leftTemp=tempLeftQueue.remove();
                rightTemp=tempRightQueue.remove();

                currentLeftScan=leftTemp.open();
                currentRightScan=rightTemp.open();
            }

            TemporaryTable[] leftTemps=new TemporaryTable[1<<hashBits];
            TemporaryTable[] rightTemps=new TemporaryTable[1<<hashBits];
            for(int i=0;i<1<<hashBits;i++){
                leftTemps[i]=new TemporaryTable(transaction,leftLayout);
                rightTemps[i]=new TemporaryTable(transaction,leftLayout);
            }
            int[] bucketLenLeft=hashBucket(currentLeftScan,leftLayout,leftHashFieldName,count,leftTemps);
            int[] bucketLenRight=hashBucket(currentRightScan,rightLayout,rightHashFieldName,count,rightTemps);
            for(int i=0;i<1<<hashBits;i++){
                if(bucketLenLeft[i]==0||bucketLenRight[i]==0){
                    //丢弃并clear
                    transaction.emptyTempFile(leftTemps[i].getFileName());
                    transaction.emptyTempFile(rightTemps[i].getFileName());
                }else if(bucketLenRight[i]<=expectedBucketLen){
                    lastTemporaryTablesLeft.add(leftTemps[i]);
                    lastTemporaryTablesRight.add(rightTemps[i]);
                }else{
                    tempLeftQueue.add(leftTemps[i]);
                    tempRightQueue.add(rightTemps[i]);
                }
            }

            //clear
            if(count!=0) {
                transaction.emptyTempFile(leftTemp.getFileName());
                transaction.emptyTempFile(rightTemp.getFileName());
            }
        }
    }

    /**
     * 分桶,删除空的桶
     * 第一次不能直接用getOriginBytes
     * 返回长度信息
     */
    public int[] hashBucket(Scan scan,Layout layout,String hashFieldName,int hashCount,
                            TemporaryTable[] buckets){
        TableScan[] tempTables=new TableScan[1<<hashBits];
        for(int i=0;i<1<<hashBits;i++){
            buckets[i]=new TemporaryTable(transaction,layout);
            tempTables[i]=buckets[i].open();
        }

        byte[] bytes=new byte[layout.getRecordSize()+ RecordPage.Flag_Size];
        while(scan.hasNext()){
            int hash = scan.getValue(hashFieldName).getValue().hashCode();
            int bucketNo=calculateBucket(hash,hashCount);
            if(scan instanceof TableScan){
                TableScan tableScan=(TableScan) scan;
                tableScan.getRecordByte(bytes);
                tempTables[bucketNo].setRecordByte(bytes);
            }else{
                for(String fieldName:layout.getFieldSet()){
                    tempTables[bucketNo].setValue(fieldName,scan.getValue(fieldName));
                }
            }
        }

        //close
        int[] retLen=new int[1<<hashBits];
        scan.close();
        for(int i=0;i<1<<hashBits;i++){
            retLen[i]=transaction.fileBlockLen(buckets[i].getFileName());
            tempTables[i].close();
        }
        return retLen;
    }

    private int calculateBucket(int hash,int hashCount){
        return hash>>>(32-(hashCount+1)*hashBits)& (1<<hashBits-1) ;
    }
}
