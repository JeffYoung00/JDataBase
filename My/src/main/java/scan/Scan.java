package scan;

import predicate.Constant;

public interface Scan {

    //结果可能为空
    void beforeFirst();

    boolean hasNext();

    Constant<?> getValue(String fieldName);

    boolean hasField(String fieldName);

    //?
    void close();
}
