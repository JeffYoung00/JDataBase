package scan;

import predicate.Constant;
import record.RecordId;

public interface UpdateScan extends Scan{

    void setValue(String fieldName, Constant<?> value);

//    void setInt(String fieldName,int value);
//
//    void setString(String fieldName,String value);

    void insert();

    void delete();

    RecordId getRecordId();
}
