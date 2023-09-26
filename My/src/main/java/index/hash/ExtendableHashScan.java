//package index.hash;
//
//import file.FileManager;
//import index.IndexScan;
//import metadata.IndexInfo;
//import record.*;
//import predicate.Constant;
//import server.Database;
//import transaction.Transaction;
//
////todo 修改写回metadata
//public class ExtendableHashScan implements IndexScan {
//
//
//    private Transaction transaction;
//    private Layout hashIndexLayout;
//
//
//    private TableScan tableScan;
//    private IndexInfo indexInfo;
//    private String fileName;
//    private String bucketFileName;
//
//    private int bucketRecordSize;
//    private int globalDepth;
//
//    BucketPage bucketPage;
//    DirPage dirPage;
//
//    public ExtendableHashScan(Transaction transaction, IndexInfo indexInfo,TableScan tableScan){
//        this.transaction=transaction;
//        this.indexInfo=indexInfo;
//        this.tableScan=tableScan;
//        this.fileName =indexInfo.getIndexName()+Database.hashIndexPostfix;
//        this.bucketFileName=indexInfo.getIndexName()+ Database.hashBucketPostfix;
//
//
//        this.hashIndexLayout= hashIndexLayout(indexInfo.getField());
//        this.globalDepth=indexInfo.getGlobalDepth();
//
//        //
//        int fileBlockLen = transaction.fileBlockLen(bucketFileName);
//        if(fileBlockLen==0){
//            transaction.appendNewFileBlock(fileName);
//            transaction.appendNewFileBlock(bucketFileName);
//        }
//
//        bucketPage=new BucketPage();
//    }
//
//
//
//    //每一轮search,根据searchKey打开一个bucket对应的tableScan
//    @Override
//    public void beforeFirst(Constant<?> searchKey) {
//        int bucketBlock = getBucketBlock(searchKey);
//        moveToBucketBlock(bucketBlock);
//        bucketPage.search(searchKey);
//
//        close();
//    }
//
//    @Override
//    public boolean hasNext() {
//        return false;
//    }
//
//    @Override
//    public RecordId getRecordId() {
//        return null;
//    }
//
//    @Override
//    public void close() {
//        if(indexTableScan!=null){
//            indexTableScan.close();
//        }
//    }
//
//    @Override
//    public void insert(Constant<?> value, RecordId recordId) {
//
//    }
//
//    @Override
//    public void delete(Constant<?> key, RecordId recordId) {
//
//    }
//
//    @Override
//    public Constant<?> getValue(String fieldName) {
//        return null;
//    }
//
//    @Override
//    public boolean hasField(String fieldName) {
//        return false;
//    }
//
//    //
//    private int getBucketBlock(Constant<?>key){
//        int hashCode=key.getValue().hashCode();
//        int bucketIndex=hashCode>>>(32-globalDepth);
//        return dirPage.read(bucketIndex);
//    }
//
//    private void moveToBucketBlock(int bucketBlock){
//        bucketPage.moveToBlock(bucketBlock);
//    }
//
//    private void split(int splitBucket){
//
//    }
//    void split(int splitBucket){
//        Bucket[] newDir=new Bucket[2<<globalDepth];
//
//        //!! 映射
//        int totalBucket=1<<globalDepth;
//        for(int i=0;i<totalBucket;i++){
//            newDir[2*i]=dir[i];
//            newDir[2*i+1]=dir[i];
//        }
//
//        Bucket newBucket = dir[splitBucket].split();
//        newDir[splitBucket*2+1]=newBucket;
//
//        dir = newDir;
//        globalDepth++;
//    }
//
//
//    public Bucket[] dir;
//
//    public ExtendableHash(int initDepth){
//        globalDepth=initDepth>0?initDepth:1;
//        dir=new Bucket[1<<globalDepth];
//
//        //global depth是最大的depth,修改global depth保证至少一个depth==global depth
//        for(int i=0;i< 1<<globalDepth;i++){
//            dir[i]=new Bucket(globalDepth);
//        }
//
//    }
//
//    void insert(IndexRecord record){
//        int hashCode=record.key.hashCode();
//        int bucketIndex=hashCode>>>(32-globalDepth);
//        if(!dir[bucketIndex].isFull()){
//            dir[bucketIndex].insertRecord(record);
//            return;
//        }
//
//        int depth=dir[bucketIndex].depth;
//
//        //
//        if(globalDepth>20){
//            System.out.println("error");
//            System.exit(0);
//        }
//
//        if(depth==globalDepth){
//            //全局分裂
//            split(bucketIndex);
//            insert(record);
//        }else{
//            //局部分裂
//            Bucket newBucket=dir[bucketIndex].split();
//
//            //如果global depth-depth==n,说明一个bucket映射到了2^n个dir,需要更新后面一半执行newBucket
//            //bucket index=110011,depth=2,现在要split,应该更新111000~111111,需要计算start position
//            //!! start position
//            //!! global depth而非32,因为这是dir index,只有global depth bit
//            int startPosition=bucketIndex>>>(globalDepth-depth)<<(globalDepth-depth);
//            int update=1<<(globalDepth-depth);
//            for (int i = update/2; i < update; i ++) {
//                dir[startPosition+i] = newBucket;
//            }
//            insert(record);
//        }
//    }
//
//    /**
//     * @param splitBucket 导致全局分裂的bucket下标,其depth==global depth且已满
//     */
//    void split(int splitBucket){
//        Bucket[] newDir=new Bucket[2<<globalDepth];
//
//        //!! 映射
//        int totalBucket=1<<globalDepth;
//        for(int i=0;i<totalBucket;i++){
//            newDir[2*i]=dir[i];
//            newDir[2*i+1]=dir[i];
//        }
//
//        Bucket newBucket = dir[splitBucket].split();
//        newDir[splitBucket*2+1]=newBucket;
//
//        dir = newDir;
//        globalDepth++;
//    }
//}
