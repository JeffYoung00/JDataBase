package scan;

import predicate.Constant;
import record.ChunkScan;
import record.Layout;
import record.Schema;
import transaction.Transaction;

public class MultiBuffersProductScan implements Scan{
    private Transaction transaction;
    private Scan leftScan;

    private String fileName;
    private Layout rightLayout;
    private int bufferSize;

    private Scan currentProoductScan=null;
    private ChunkScan currentChunkScan=null;
    private int nextRightBlockNumber;

    private int rightBlockLen;

    /**
     * @param rightLayout     让上层传layout,因为上层会使用多个product,避免重复layout
     *     layout用来造chunk scan
     * fileName,因为chunk scan直接打开file
     */
    public MultiBuffersProductScan(Transaction transaction, Layout rightLayout, Scan leftScan, String fileName,int bufferSize){
        this.transaction=transaction;
        this.rightLayout = rightLayout;
        this.leftScan=leftScan;
        this.fileName=fileName;
        this.bufferSize=bufferSize;

        this.rightBlockLen =transaction.fileBlockLen(fileName);
    }


    public MultiBuffersProductScan(Transaction transaction, Schema schema, Scan leftScan, String fileName,int bufferSize){
        this(transaction,new Layout(schema),leftScan,fileName,bufferSize);
    }


    @Override
    public void beforeFirst() {
        nextRightBlockNumber=0;
        useNextChunk();
        currentProoductScan.beforeFirst();
    }

    //关闭上一个chunk scan
    boolean useNextChunk(){
        if(nextRightBlockNumber>=rightBlockLen){
            return false;
        }
        if(currentChunkScan!=null){
            currentChunkScan.close();
        }

        int start=nextRightBlockNumber;
        int end=Math.min(nextRightBlockNumber+bufferSize,rightBlockLen);
        currentChunkScan=new ChunkScan(transaction, rightLayout,fileName,start,end);
        currentProoductScan=new ProductScan(leftScan,currentChunkScan);
        nextRightBlockNumber=end;
        return true;
    }

    @Override
    public boolean hasNext() {
        while(!currentProoductScan.hasNext()){
            if(!useNextChunk()){
                return false;
            }
        }
        return true;
    }

    @Override
    public Constant<?> getValue(String fieldName) {
        return currentProoductScan.getValue(fieldName);
    }

    @Override
    public boolean hasField(String fieldName) {
        return currentProoductScan.hasField(fieldName);
    }

    @Override
    public void close() {
        currentProoductScan.close();
    }
}
