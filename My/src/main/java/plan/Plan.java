package plan;

import record.Schema;
import scan.Scan;

public interface Plan {

    Scan open();

    /**
     * 一次遍历需要访问多少个块
     */
    int getBlockAccessedNumber();

    int getRecordNumber();

    int getFieldDistinctValues(String fieldName);

    /**
     * 整个操作的预先花销
     **/
    int cost();

    Schema getSchema();
}

