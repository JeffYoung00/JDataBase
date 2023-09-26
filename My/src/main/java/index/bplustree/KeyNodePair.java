package index.bplustree;

import lombok.Getter;
import predicate.Constant;

public class KeyNodePair {
    @Getter private Constant<?> key;
    @Getter private int blockNumber;
    public KeyNodePair(Constant<?>key,int blockNumber){
        this.key=key;
        this.blockNumber=blockNumber;
    }
}
