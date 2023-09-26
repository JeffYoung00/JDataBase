package index.hash;

import file.BlockId;
import lombok.Getter;
import predicate.Constant;

public class HashIndexEntry {
//    @Getter Constant<?> key;
    @Getter private int hashCode;
    @Getter private int blockNumber;
    @Getter private int slotNumber;
    public HashIndexEntry(int hashCode,int blockNumber,int slotNumber){
//        this.key=key;
        this.hashCode=hashCode;
        this.blockNumber=blockNumber;
        this.slotNumber=slotNumber;
    }

    boolean isOneInNthBit(int n){
        return ((hashCode>>>(32-1-n))&1)==1;
    }
}
