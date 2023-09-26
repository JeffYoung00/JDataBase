package transaction.recovery.logrecord;

import file.Page;
import transaction.Transaction;

public interface LogRecord {

    int CHECKPOINT=0,START= 1, COMMIT = 2, ROLLBACK = 3, SET_INT = 4,SET_STRING=5;

    int type();

    int transactionId();
}
