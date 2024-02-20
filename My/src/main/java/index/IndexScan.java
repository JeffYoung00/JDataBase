package index;

import predicate.Constant;
import record.RecordId;

public interface IndexScan {
    //找目标数据
    void beforeFirst(Constant<?>key);
    boolean hasNext();
    RecordId getRecordId();
    void close();

    void insert(Constant<?>key,RecordId recordId);

    void delete(Constant<?>key,RecordId recordId);

    //......
    Constant<?> getValue(String fieldName);

    boolean hasField(String fieldName);


}
