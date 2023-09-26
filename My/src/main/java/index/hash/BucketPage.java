//package index.hash;
//
//import file.BlockId;
//import file.FileManager;
//import predicate.Constant;
//import record.Field;
//import record.Layout;
//import record.Schema;
//import transaction.Transaction;
//
//import java.util.Arrays;
//import java.util.List;
//import java.util.Objects;
//
////depth count IndexRecord
//
////todo 只有hashcode没有key
//// hashCode排序,快速split ->没有必要
//class BucketPage{
//
//    Transaction transaction;
//    int maxRecord;
//
//    int depth;
//    int recordCount;
//
////    int keyType;
//    Layout indexLayout;
//    BlockId blockId;
//
//    static Layout hashIndexLayout(Field field){
//        Schema indexSchema=new Schema();
////        indexSchema.addField(new Field("key", field.getType(), field.getLen()));
//        indexSchema.addField(new Field("hash",Field.Integer));
//        indexSchema.addField(new Field("blockNumber",Field.Integer));
//        indexSchema.addField(new Field("slotNumber",Field.Integer));
//        return new Layout(indexSchema);
//    }
//
//    BucketPage(Transaction transaction, Layout indexLayout,BlockId blockId){
//        this.transaction=transaction;
//
//        this.indexLayout=indexLayout;
//        this.blockId=blockId;
//
////        this.keyType=indexLayout.getType("key");
//        transaction.pin(blockId);
//        this.maxRecord=(FileManager.BLOCK_SIZE-8)/indexLayout.getRecordSize();
//        this.depth=transaction.getInt(blockId,0);
//        this.recordCount=transaction.getInt(blockId,4);
//    }
//
//    public void changeBlock(int blockNumber){
//        transaction.unpin(blockId);
//        blockId.setBlockNumber(blockNumber);
//        transaction.pin(blockId);
//        this.depth=transaction.getInt(blockId,0);
//        this.recordCount=transaction.getInt(blockId,4);
//    }
//
//    boolean isFull(){
//        return maxRecord==recordCount;
//    }
//
//    void insertRecord(HashIndexEntry record){
//        setRecord(recordCount,record);
//        recordCount++;
//    }
//
//    int calculateOffset(int recordCount){
//        return 8+recordCount*indexLayout.getRecordSize();
//    }
//
//    private void setRecord(int recordCount,HashIndexEntry record){
//        int startOffset=calculateOffset(recordCount);
//        transaction.setInt(blockId,startOffset+indexLayout.getOffset("hash"),record.getHashCode(),true);
//        transaction.setInt(blockId,startOffset+indexLayout.getOffset("blockNumber"),record.getBlockNumber(), true);
//        transaction.setInt(blockId,startOffset+indexLayout.getOffset("slotNumber"),record.getSlotNumber(), true);
//    }
//
//    private HashIndexEntry getRecord(int recordCount){
//        int startOffset=calculateOffset(recordCount);
////        if(keyType==Field.Integer){
////            transaction.setInt(blockId,startOffset+indexLayout.getOffset("key"),(int)record.getKey().getValue(),true);
////        }else{
////            transaction.setString(blockId,startOffset+indexLayout.getOffset("key"),(String)record.getKey().getValue(),true);
////        }
//        int hash = transaction.getInt(blockId, startOffset + indexLayout.getOffset("hash"));
//        int blockNumber = transaction.getInt(blockId, startOffset + indexLayout.getOffset("blockNumber"));
//        int slotNumber = transaction.getInt(blockId, startOffset + indexLayout.getOffset("slotNumber"));
//        return new HashIndexEntry(hash,blockNumber,slotNumber);
//    }
//
//    public List<HashIndexEntry> search(int hashCode){
//        for(int i=0;i<recordCount;i++){
//
//        }
//    }
//
//
//    //最初的两个bucket
//    BucketPage(int initDepth){
//        depth=initDepth;
//        recordCount=0;
//        records=new IndexRecord[ExtendableHash.Slot_Size];
//    }
//
//    BucketPage(int depth,int recordCount, IndexRecord[]records){
//        this.depth=depth;
//        this.recordCount=recordCount;
//        this.records=records;
//    }
//
//
//
//
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
//}
