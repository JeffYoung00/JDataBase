package file;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@ToString
@EqualsAndHashCode
public class BlockId {
    private String fileName;
    private int blockNumber;

    public BlockId(String fileName, int blockNumber){
        this.blockNumber=blockNumber;
        this.fileName=fileName;
    }

    /**
     * 谨慎使用
     */
    public void setBlockNumber(int blockNumber) {
        this.blockNumber = blockNumber;
    }
}
