//package index.hash;
//
//import lombok.ToString;
//
//import java.util.Arrays;
//import java.util.Objects;
//
//@ToString
//public class ExtendableHash {
//
//    //一个block/bucket/file中的index条数
//    static int Slot_Size=2;
//
//    public Bucket[] dir;
//    public int globalDepth;
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
//    IndexRecord search(String key){
//        int hashCode=key.hashCode();
//        int bucketIndex=hashCode>>>(32-globalDepth);
//        return dir[bucketIndex].search(key);
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
//
//    public static void main(String[] args) {
//        ExtendableHash extendableHash=new ExtendableHash(1);
//        IndexRecord r1=new IndexRecord("thisisfirst",1,1);
//        IndexRecord r2=new IndexRecord("thisissecond",1,1);
//        IndexRecord r3=new IndexRecord("thisisthird",1,1);
//        IndexRecord r4=new IndexRecord("thisisforth",1,1);
//        IndexRecord r5=new IndexRecord("thisisfifth",1,1);
//        IndexRecord r6=new IndexRecord("thisissixth",1,1);
//        IndexRecord r7=new IndexRecord("thisisseventh",1,1);
//        IndexRecord r8=new IndexRecord("thisiseighth",1,1);
//
////        System.out.println(Integer.toBinaryString(r1.hashCode));
////        System.out.println(Integer.toBinaryString(r2.hashCode));
////        System.out.println(Integer.toBinaryString(r3.hashCode));
////        System.out.println(Integer.toBinaryString(r4.hashCode));
////        System.out.println(Integer.toBinaryString(r5.hashCode));
////        System.out.println(Integer.toBinaryString(r6.hashCode));
////        System.out.println(Integer.toBinaryString(r7.hashCode));
////        System.out.println(Integer.toBinaryString(r8.hashCode));
//
//
////        10101100001010
////        111
////        101011001
////        10101100001011
////        10101100001010
////        101011001
////        111
////        110
//
//        //5个bucket用了int[2^14],64KB
//
//        extendableHash.insert(r1);
//        extendableHash.insert(r2);
//        extendableHash.insert(r3);
//        extendableHash.insert(r4);
//        extendableHash.insert(r5);
//        extendableHash.insert(r6);
//        extendableHash.insert(r7);
//        extendableHash.insert(r8);
//        extendableHash.output();
//
//    }
//
//    public void output(){
//        System.out.println(globalDepth);
//        for(int i=0;i<dir.length;i++){
//            System.out.println(i+"  :"+dir[i]);
//        }
//    }
//}
//
//class Bucket{
//    int depth;
//    IndexRecord[] records;
//    int recordCount;
//
//    //最初的两个bucket
//    Bucket(int initDepth){
//        depth=initDepth;
//        recordCount=0;
//        records=new IndexRecord[ExtendableHash.Slot_Size];
//    }
//
//    Bucket(int depth,int recordCount, IndexRecord[]records){
//        this.depth=depth;
//        this.recordCount=recordCount;
//        this.records=records;
//    }
//
//    void insertRecord(IndexRecord record){
//
//        for(int i=0;i<ExtendableHash.Slot_Size;i++){
//            if(records[i]==null){
//                records[i]=record;
//                break;
//            }
//        }
//        recordCount++;
//    }
//
//    boolean isFull(){
//        return ExtendableHash.Slot_Size==recordCount;
//    }
//
//    public IndexRecord search(String key){
//        int hashCode=key.hashCode();
//        for(int i=0;i<ExtendableHash.Slot_Size;i++){
//            if(records[i]!=null&&records[i].hashCode==hashCode&& Objects.equals(key,records[i].key)){
//                return records[i];
//            }
//        }
//        return null;
//    }
//
//    //将自己的一部分分出去
//    Bucket split(){
//        IndexRecord[] splitRecords=new IndexRecord[ExtendableHash.Slot_Size];
//        int index=0;
//        for(int i=0;i<ExtendableHash.Slot_Size;i++){
//            //当前使用了0~depth-1 bit,现在判断 depth bit是否为1
//            if(records[i].isOneInNthBit(depth)){
//                splitRecords[index++]=records[i];
//                records[i]=null;
//
//                //!! 重置
//                recordCount--;
//            }
//        }
//        depth++;
//
//        //!! 新bucket的recordCount
//        return new Bucket(depth,index,splitRecords);
//    }
//
//    @Override
//    public String toString() {
//        return "depth=" + depth +
//                ", records=" + Arrays.toString(records) +
//                ", recordCount=" + recordCount ;
//    }
//}
//
//class IndexRecord{
//
//    String key;
//    int hashCode;
//    int blockId;
//    int slotId;
//
//    IndexRecord(String key, int blockId, int slotId){
//        this.key = key;
//        this.hashCode= key.hashCode();
//        this.blockId=blockId;
//        this.slotId=slotId;
//    }
//
//    //hashcode的第n bit 是不是1
//    boolean isOneInNthBit(int n){
//        return ((hashCode>>>(32-1-n))&1)==1;
//    }
//
//    @Override
//    public String toString() {
//        return "IndexRecord{" +
//                "key='" + key + '\'' +
//                ", hashCode=" + Integer.toHexString(hashCode) +
//                ", blockId=" + blockId +
//                ", slotId=" + slotId +
//                '}';
//    }
//}
