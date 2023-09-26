package record;

import file.BlockId;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class RecordId {
    private int blockNumber;
    private int slot;
    public RecordId(int blockNumber,int slot){
        this.blockNumber=blockNumber;
        this.slot=slot;
    }
}
