package index.hash;

import file.BlockId;
import file.FileManager;
import metadata.IndexInfo;
import server.Database;
import transaction.Transaction;

/**
 * hash index block bucket: []
 *
 * - 每个块的数据量是固定的4096/16=256, 最大一共24bit用于hash
 *
 * - 每个桶的数据量是固定的4096/4=1024, depth<10时bucket还是在一个page中分裂
 */
/**
 * 用末尾的hash更好,这样xxx0,xxx1分裂为xxx00,xxx01,xxx10,xxx11,直接往下复制一份
 */
/**
 * bucket file应该由一个独立的transaction来管理?? 桶分裂时,transaction还是会对bucket做出一个桶的修改 (x)
 * eg:事务插入点a,split,插入点b -> 记录两个插入是无法回滚的, 必须一开始就判断会不会造成split
 * 如果不记录split log,就需要一开始就决定split,但如果连着insert n多次,怎么可能一开始就知道会如何split呢
 * 因为有split log,所以读写锁还是正常加上(级联回滚),否则提前释放写锁,其他事务再split...
 */
class BucketPage{
    public static int Init_Depth=0;

    static int BucketOrder=FileManager.BLOCK_SIZE/4;

    Transaction transaction;
    IndexInfo indexInfo;
    BlockId blockId=null;
    String fileName;

    BucketPage(Transaction transaction, IndexInfo indexInfo){
        this.transaction=transaction;
        this.indexInfo=indexInfo;
        this.fileName=indexInfo.getIndexName()+ Database.hashBucketPostfix;
    }

    void close(){
        transaction.unpin(blockId);
    }

    public int getBlockNoByHash(int hash){
        int mask=1<<indexInfo.getGlobalDepth()-1;
        hash&=mask;
        int bucketBlock=hash/BucketOrder;
        int bucketSlot=hash%BucketOrder;
        moveToBlockNumber(bucketBlock);
        return transaction.getInt(blockId,calculateOffset(bucketSlot));
    }

    int calculateOffset(int slotNumber){
        return slotNumber*4;
    }

    void moveToBlockNumber(int bucketBlock){
        if(blockId==null){
            blockId=new BlockId(fileName,bucketBlock);
            transaction.pin(blockId);
        }else if(blockId.getBlockNumber()!=bucketBlock){
            transaction.unpin(blockId);
            blockId.setBlockNumber(bucketBlock);
            transaction.pin(blockId);
        }
    }

    /**
     * 表锁不需要,fileBlockLen有EOF锁
     */
    void split(){
        int count=1<<indexInfo.getGlobalDepth();
        if(count<BucketOrder){//再一个block中split
            for(int index=0;index<count;index++){
                transaction.setInt(blockId,calculateOffset(count+index), transaction.getInt(blockId,calculateOffset(index)),false);
            }
        }else{//申请block然后split
            int targetLen= (1<<indexInfo.getGlobalDepth()<<1)/BucketOrder;
            while(transaction.fileBlockLen(fileName)<targetLen){
                transaction.appendNewFileBlock(fileName);
            }

            for(int i=0;i<targetLen/2;i++){
                BlockId fromBlock=new BlockId(fileName,i);
                BlockId toBlock=new BlockId(fileName,targetLen/2+i);
                transaction.pin(fromBlock);
                transaction.pin(toBlock);
                for(int index=0;index<BucketOrder;index++){
                    transaction.setInt(toBlock,calculateOffset(index),transaction.getInt(fromBlock,calculateOffset(index)),false);
                }
                transaction.unpin(fromBlock);
                transaction.unpin(toBlock);
            }
        }
        //todo metadata
        indexInfo.growGlobalDepth();
    }

    public void setBlockNoByHash(int hash,int targetBlock){
        int bucketBlock=hash/BucketOrder;
        int bucketSlot=hash%BucketOrder;
        moveToBlockNumber(bucketBlock);
        transaction.setInt(blockId,calculateOffset(bucketSlot),targetBlock,true);
    }

    /**
     * 原本 xxx111=a --> xx0111=a xx1111=target
     *
     * 先修改localDepth,得到targetBlock,然后split修改globalDepth
     */
    void localSplit(int hash,int targetBlock,int localDepth){
        if(localDepth>indexInfo.getGlobalDepth()){
            split();
        }
        hash&=1<<(localDepth-1)-1;
        hash+=1<<(localDepth-1);
        int max=1<<indexInfo.getGlobalDepth();
        for(;hash<max;hash+=1<<localDepth){
            setBlockNoByHash(hash,targetBlock);
        }
    }
}
