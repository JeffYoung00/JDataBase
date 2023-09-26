package transaction.recovery.logrecord;

import transaction.Transaction;

public interface UpdateLogRecord extends LogRecord{
    void undo(Transaction transaction);

    void redo(Transaction transaction);
}
