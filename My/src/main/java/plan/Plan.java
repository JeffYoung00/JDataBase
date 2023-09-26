package plan;


import record.Schema;
import scan.Scan;

/**
 * The interface implemented by each query plan.
 * There is a Plan class for each relational algebra operator.
 * @author Edward Sciore
 *
 */
public interface Plan {

    Scan open();

    int    getBlockAccessedNumber();

    int    getRecordNumber();

    int    getFieldDistinctValues(String fieldName);

    int cost();

    Schema getSchema();
}

