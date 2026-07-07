package edu.berkeley.cs186.database.recovery;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.concurrency.DummyLockContext;
import edu.berkeley.cs186.database.io.DiskSpaceManager;
import edu.berkeley.cs186.database.memory.BufferManager;
import edu.berkeley.cs186.database.memory.Page;
import edu.berkeley.cs186.database.recovery.records.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * Implementation of ARIES.
 */
public class ARIESRecoveryManager implements RecoveryManager {
    // Disk space manager.
    DiskSpaceManager diskSpaceManager;
    // Buffer manager.
    BufferManager bufferManager;

    // Function to create a new transaction for recovery with a given
    // transaction number.
    private Function<Long, Transaction> newTransaction;

    // Log manager
    LogManager logManager;
    // Dirty page table (page number -> recLSN).
    Map<Long, Long> dirtyPageTable = new ConcurrentHashMap<>();
    // Transaction table (transaction number -> entry).
    Map<Long, TransactionTableEntry> transactionTable = new ConcurrentHashMap<>();
    // true if redo phase of restart has terminated, false otherwise. Used
    // to prevent DPT entries from being flushed during restartRedo.
    boolean redoComplete;

    public ARIESRecoveryManager(Function<Long, Transaction> newTransaction) {
        this.newTransaction = newTransaction;
    }

    /**
     * Initializes the log; only called the first time the database is set up.
     * The master record should be added to the log, and a checkpoint should be
     * taken.
     */
    @Override
    public void initialize() {
        this.logManager.appendToLog(new MasterLogRecord(0));
        this.checkpoint();
    }

    /**
     * Sets the buffer/disk managers. This is not part of the constructor
     * because of the cyclic dependency between the buffer manager and recovery
     * manager (the buffer manager must interface with the recovery manager to
     * block page evictions until the log has been flushed, but the recovery
     * manager needs to interface with the buffer manager to write the log and
     * redo changes).
     * @param diskSpaceManager disk space manager
     * @param bufferManager buffer manager
     */
    @Override
    public void setManagers(DiskSpaceManager diskSpaceManager, BufferManager bufferManager) {
        this.diskSpaceManager = diskSpaceManager;
        this.bufferManager = bufferManager;
        this.logManager = new LogManager(bufferManager);
    }

    // Forward Processing //////////////////////////////////////////////////////

    /**
     * Called when a new transaction is started.
     *
     * The transaction should be added to the transaction table.
     *
     * @param transaction new transaction
     */
    @Override
    public synchronized void startTransaction(Transaction transaction) {
        this.transactionTable.put(transaction.getTransNum(), new TransactionTableEntry(transaction));
    }

    /**
     * Called when a transaction is about to start committing.
     *
     * A commit record should be appended, the log should be flushed,
     * and the transaction table and the transaction status should be updated.
     *
     * @param transNum transaction being committed
     * @return LSN of the commit record
     */
    @Override
    public long commit(long transNum) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        // Append a commit record, flush the log (WAL: commit must be durable),
        // and update the transaction table + status.
        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new CommitTransactionLogRecord(transNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        transactionEntry.lastLSN = LSN;
        logManager.flushToLSN(LSN);
        transactionEntry.transaction.setStatus(Transaction.Status.COMMITTING);
        return LSN;
    }

    /**
     * Called when a transaction is set to be aborted.
     *
     * An abort record should be appended, and the transaction table and
     * transaction status should be updated. Calling this function should not
     * perform any rollbacks.
     *
     * @param transNum transaction being aborted
     * @return LSN of the abort record
     */
    @Override
    public long abort(long transNum) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        // Append an abort record and mark the transaction ABORTING. No rollback
        // happens here — that occurs in end().
        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new AbortTransactionLogRecord(transNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        transactionEntry.lastLSN = LSN;
        transactionEntry.transaction.setStatus(Transaction.Status.ABORTING);
        return LSN;
    }

    /**
     * Called when a transaction is cleaning up; this should roll back
     * changes if the transaction is aborting (see the rollbackToLSN helper
     * function below).
     *
     * Any changes that need to be undone should be undone, the transaction should
     * be removed from the transaction table, the end record should be appended,
     * and the transaction status should be updated.
     *
     * @param transNum transaction to end
     * @return LSN of the end record
     */
    @Override
    public long end(long transNum) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        // If aborting, roll back all of this transaction's changes to the start
        // (LSN 0) before ending.
        if (transactionEntry.transaction.getStatus() == Transaction.Status.ABORTING) {
            rollbackToLSN(transNum, 0);
        }
        // Append the end record, mark COMPLETE, and remove from the table.
        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new EndTransactionLogRecord(transNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        transactionEntry.transaction.setStatus(Transaction.Status.COMPLETE);
        transactionTable.remove(transNum);
        return LSN;
    }

    /**
     * Recommended helper function: performs a rollback of all of a
     * transaction's actions, up to (but not including) a certain LSN.
     * Starting with the LSN of the most recent record that hasn't been undone:
     * - while the current LSN is greater than the LSN we're rolling back to:
     *    - if the record at the current LSN is undoable:
     *       - Get a compensation log record (CLR) by calling undo on the record
     *       - Append the CLR
     *       - Call redo on the CLR to perform the undo
     *    - update the current LSN to that of the next record to undo
     *
     * Note above that calling .undo() on a record does not perform the undo, it
     * just creates the compensation log record.
     *
     * @param transNum transaction to perform a rollback for
     * @param LSN LSN to which we should rollback
     */
    private void rollbackToLSN(long transNum, long LSN) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        LogRecord lastRecord = logManager.fetchLogRecord(transactionEntry.lastLSN);
        long lastRecordLSN = lastRecord.getLSN();
        // Small optimization: if the last record is a CLR we can start rolling
        // back from the next record that hasn't yet been undone.
        long currentLSN = lastRecord.getUndoNextLSN().orElse(lastRecordLSN);
        // Walk backwards through the transaction's records, undoing each undoable
        // one by writing a CLR and applying it, until we pass the target LSN.
        while (currentLSN > LSN) {
            LogRecord record = logManager.fetchLogRecord(currentLSN);
            if (record.isUndoable()) {
                // undo() builds the compensation log record (CLR); we append it,
                // update the transaction's lastLSN, then redo() the CLR to
                // actually apply the undo.
                LogRecord clr = record.undo(transactionEntry.lastLSN);
                long clrLSN = logManager.appendToLog(clr);
                transactionEntry.lastLSN = clrLSN;
                clr.redo(this, diskSpaceManager, bufferManager);
            }
            // Move to the next record to undo: follow undoNextLSN if present
            // (skips already-undone records), otherwise the prevLSN.
            currentLSN = record.getUndoNextLSN().orElse(record.getPrevLSN().orElse(0L));
        }
    }

    /**
     * Called before a page is flushed from the buffer cache. This
     * method is never called on a log page.
     *
     * The log should be as far as necessary.
     *
     * @param pageLSN pageLSN of page about to be flushed
     */
    @Override
    public void pageFlushHook(long pageLSN) {
        logManager.flushToLSN(pageLSN);
    }

    /**
     * Called when a page has been updated on disk.
     *
     * As the page is no longer dirty, it should be removed from the
     * dirty page table.
     *
     * @param pageNum page number of page updated on disk
     */
    @Override
    public void diskIOHook(long pageNum) {
        if (redoComplete) dirtyPageTable.remove(pageNum);
    }

    /**
     * Called when a write to a page happens.
     *
     * This method is never called on a log page. Arguments to the before and after params
     * are guaranteed to be the same length.
     *
     * The appropriate log record should be appended, and the transaction table
     * and dirty page table should be updated accordingly.
     *
     * @param transNum transaction performing the write
     * @param pageNum page number of page being written
     * @param pageOffset offset into page where write begins
     * @param before bytes starting at pageOffset before the write
     * @param after bytes starting at pageOffset after the write
     * @return LSN of last record written to log
     */
    @Override
    public long logPageWrite(long transNum, long pageNum, short pageOffset, byte[] before,
                             byte[] after) {
        assert (before.length == after.length);
        assert (before.length <= BufferManager.EFFECTIVE_PAGE_SIZE / 2);
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);

        // Append an update record, advancing the transaction's lastLSN.
        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new UpdatePageLogRecord(transNum, pageNum, prevLSN, pageOffset,
                before, after);
        long LSN = logManager.appendToLog(record);
        transactionEntry.lastLSN = LSN;

        // The page is now dirty in memory as of this LSN (recLSN if not already
        // dirty). dirtyPage() records the earliest LSN that dirtied the page.
        dirtyPage(pageNum, LSN);
        return LSN;
    }

    /**
     * Called when a new partition is allocated. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the partition is the log partition.
     *
     * The appropriate log record should be appended, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the allocation
     * @param partNum partition number of the new partition
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logAllocPart(long transNum, int partNum) {
        // Ignore if part of the log.
        if (partNum == 0) return -1L;
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new AllocPartLogRecord(transNum, partNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a partition is freed. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the partition is the log partition.
     *
     * The appropriate log record should be appended, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the partition be freed
     * @param partNum partition number of the partition being freed
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logFreePart(long transNum, int partNum) {
        // Ignore if part of the log.
        if (partNum == 0) return -1L;

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new FreePartLogRecord(transNum, partNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a new page is allocated. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the page is in the log partition.
     *
     * The appropriate log record should be appended, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the allocation
     * @param pageNum page number of the new page
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logAllocPage(long transNum, long pageNum) {
        // Ignore if part of the log.
        if (DiskSpaceManager.getPartNum(pageNum) == 0) return -1L;

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new AllocPageLogRecord(transNum, pageNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a page is freed. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the page is in the log partition.
     *
     * The appropriate log record should be appended, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the page be freed
     * @param pageNum page number of the page being freed
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logFreePage(long transNum, long pageNum) {
        // Ignore if part of the log.
        if (DiskSpaceManager.getPartNum(pageNum) == 0) return -1L;

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new FreePageLogRecord(transNum, pageNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        dirtyPageTable.remove(pageNum);
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Creates a savepoint for a transaction. Creating a savepoint with
     * the same name as an existing savepoint for the transaction should
     * delete the old savepoint.
     *
     * The appropriate LSN should be recorded so that a partial rollback
     * is possible later.
     *
     * @param transNum transaction to make savepoint for
     * @param name name of savepoint
     */
    @Override
    public void savepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);
        transactionEntry.addSavepoint(name);
    }

    /**
     * Releases (deletes) a savepoint for a transaction.
     * @param transNum transaction to delete savepoint for
     * @param name name of savepoint
     */
    @Override
    public void releaseSavepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);
        transactionEntry.deleteSavepoint(name);
    }

    /**
     * Rolls back transaction to a savepoint.
     *
     * All changes done by the transaction since the savepoint should be undone,
     * in reverse order, with the appropriate CLRs written to log. The transaction
     * status should remain unchanged.
     *
     * @param transNum transaction to partially rollback
     * @param name name of savepoint
     */
    @Override
    public void rollbackToSavepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        // All of the transaction's changes strictly after the record at LSN should be undone.
        long savepointLSN = transactionEntry.getSavepoint(name);

        // Undo everything after the savepoint; status is left unchanged.
        rollbackToLSN(transNum, savepointLSN);
    }

    /**
     * Create a checkpoint.
     *
     * First, a begin checkpoint record should be written.
     *
     * Then, end checkpoint records should be filled up as much as possible first
     * using recLSNs from the DPT, then status/lastLSNs from the transactions
     * table, and written when full (or when nothing is left to be written).
     * You may find the method EndCheckpointLogRecord#fitsInOneRecord here to
     * figure out when to write an end checkpoint record.
     *
     * Finally, the master record should be rewritten with the LSN of the
     * begin checkpoint record.
     */
    @Override
    public synchronized void checkpoint() {
        // Create begin checkpoint log record and write to log
        LogRecord beginRecord = new BeginCheckpointLogRecord();
        long beginLSN = logManager.appendToLog(beginRecord);

        Map<Long, Long> chkptDPT = new HashMap<>();
        Map<Long, Pair<Transaction.Status, Long>> chkptTxnTable = new HashMap<>();

        // Fill end checkpoint records with DPT entries first. Whenever adding the
        // next DPT entry would overflow one record, flush the current one and
        // start fresh.
        for (Map.Entry<Long, Long> entry : dirtyPageTable.entrySet()) {
            if (!EndCheckpointLogRecord.fitsInOneRecord(chkptDPT.size() + 1, 0)) {
                LogRecord endRecord = new EndCheckpointLogRecord(chkptDPT, chkptTxnTable);
                logManager.appendToLog(endRecord);
                chkptDPT = new HashMap<>();
            }
            chkptDPT.put(entry.getKey(), entry.getValue());
        }

        // Then add transaction table entries (status + lastLSN). When the next
        // entry won't fit alongside the current DPT + txn entries, flush.
        for (Map.Entry<Long, TransactionTableEntry> entry : transactionTable.entrySet()) {
            long transNum = entry.getKey();
            TransactionTableEntry tte = entry.getValue();
            if (!EndCheckpointLogRecord.fitsInOneRecord(chkptDPT.size(), chkptTxnTable.size() + 1)) {
                LogRecord endRecord = new EndCheckpointLogRecord(chkptDPT, chkptTxnTable);
                logManager.appendToLog(endRecord);
                chkptDPT = new HashMap<>();
                chkptTxnTable = new HashMap<>();
            }
            chkptTxnTable.put(transNum,
                    new Pair<>(tte.transaction.getStatus(), tte.lastLSN));
        }

        // Last end checkpoint record
        LogRecord endRecord = new EndCheckpointLogRecord(chkptDPT, chkptTxnTable);
        logManager.appendToLog(endRecord);
        // Ensure checkpoint is fully flushed before updating the master record
        flushToLSN(endRecord.getLSN());

        // Update master record
        MasterLogRecord masterRecord = new MasterLogRecord(beginLSN);
        logManager.rewriteMasterRecord(masterRecord);
    }

    /**
     * Flushes the log to at least the specified record,
     * essentially flushing up to and including the page
     * that contains the record specified by the LSN.
     *
     * @param LSN LSN up to which the log should be flushed
     */
    @Override
    public void flushToLSN(long LSN) {
        this.logManager.flushToLSN(LSN);
    }

    @Override
    public void dirtyPage(long pageNum, long LSN) {
        dirtyPageTable.putIfAbsent(pageNum, LSN);
        // Handle race condition where earlier log is beaten to the insertion by
        // a later log.
        dirtyPageTable.computeIfPresent(pageNum, (k, v) -> Math.min(LSN,v));
    }

    @Override
    public void close() {
        this.checkpoint();
        this.logManager.close();
    }

    // Restart Recovery ////////////////////////////////////////////////////////

    /**
     * Called whenever the database starts up, and performs restart recovery.
     * Recovery is complete when the Runnable returned is run to termination.
     * New transactions may be started once this method returns.
     *
     * This should perform the three phases of recovery, and also clean the
     * dirty page table of non-dirty pages (pages that aren't dirty in the
     * buffer manager) between redo and undo, and perform a checkpoint after
     * undo.
     */
    @Override
    public void restart() {
        this.restartAnalysis();
        this.restartRedo();
        this.redoComplete = true;
        this.cleanDPT();
        this.restartUndo();
        this.checkpoint();
    }

    /**
     * This method performs the analysis pass of restart recovery.
     *
     * First, the master record should be read (LSN 0). The master record contains
     * one piece of information: the LSN of the last successful checkpoint.
     *
     * We then begin scanning log records, starting at the beginning of the
     * last successful checkpoint.
     *
     * If the log record is for a transaction operation (getTransNum is present)
     * - update the transaction table
     *
     * If the log record is page-related (getPageNum is present), update the dpt
     *   - update/undoupdate page will dirty pages
     *   - free/undoalloc page always flush changes to disk
     *   - no action needed for alloc/undofree page
     *
     * If the log record is for a change in transaction status:
     * - update transaction status to COMMITTING/RECOVERY_ABORTING/COMPLETE
     * - update the transaction table
     * - if END_TRANSACTION: clean up transaction (Transaction#cleanup), remove
     *   from txn table, and add to endedTransactions
     *
     * If the log record is an end_checkpoint record:
     * - Copy all entries of checkpoint DPT (replace existing entries if any)
     * - Skip txn table entries for transactions that have already ended
     * - Add to transaction table if not already present
     * - Update lastLSN to be the larger of the existing entry's (if any) and
     *   the checkpoint's
     * - The status's in the transaction table should be updated if it is possible
     *   to transition from the status in the table to the status in the
     *   checkpoint. For example, running -> aborting is a possible transition,
     *   but aborting -> running is not.
     *
     * After all records in the log are processed, for each ttable entry:
     *  - if COMMITTING: clean up the transaction, change status to COMPLETE,
     *    remove from the ttable, and append an end record
     *  - if RUNNING: change status to RECOVERY_ABORTING, and append an abort
     *    record
     *  - if RECOVERY_ABORTING: no action needed
     */
    void restartAnalysis() {
        // Read master record
        LogRecord record = logManager.fetchLogRecord(0L);
        // Type checking
        assert (record != null && record.getType() == LogType.MASTER);
        MasterLogRecord masterRecord = (MasterLogRecord) record;
        // Get start checkpoint LSN
        long LSN = masterRecord.lastCheckpointLSN;
        // Set of transactions that have completed
        Set<Long> endedTransactions = new HashSet<>();

        // Scan the log forward from the last checkpoint.
        Iterator<LogRecord> iter = logManager.scanFrom(LSN);
        while (iter.hasNext()) {
            LogRecord logRecord = iter.next();
            long recordLSN = logRecord.getLSN();

            // --- Transaction-operation records: ensure the txn is in the table
            // and advance its lastLSN.
            if (logRecord.getTransNum().isPresent()) {
                long transNum = logRecord.getTransNum().get();
                if (!transactionTable.containsKey(transNum)) {
                    startTransaction(newTransaction.apply(transNum));
                }
                transactionTable.get(transNum).lastLSN = recordLSN;
            }

            // --- Page-related records: maintain the dirty page table.
            if (logRecord.getPageNum().isPresent()) {
                long pageNum = logRecord.getPageNum().get();
                LogType type = logRecord.getType();
                switch (type) {
                    case UPDATE_PAGE:
                    case UNDO_UPDATE_PAGE:
                        // These dirty the page (record its recLSN if new).
                        dirtyPage(pageNum, recordLSN);
                        break;
                    case FREE_PAGE:
                    case UNDO_ALLOC_PAGE:
                        // These flush the page to disk, so it's clean.
                        dirtyPageTable.remove(pageNum);
                        break;
                    case ALLOC_PAGE:
                    case UNDO_FREE_PAGE:
                        // No effect on the dirty page table.
                        break;
                    default:
                        break;
                }
            }

            // --- Transaction-status change records.
            LogType type = logRecord.getType();
            if (type == LogType.COMMIT_TRANSACTION || type == LogType.ABORT_TRANSACTION
                    || type == LogType.END_TRANSACTION) {
                long transNum = logRecord.getTransNum().get();
                TransactionTableEntry entry = transactionTable.get(transNum);
                if (type == LogType.COMMIT_TRANSACTION) {
                    entry.transaction.setStatus(Transaction.Status.COMMITTING);
                } else if (type == LogType.ABORT_TRANSACTION) {
                    entry.transaction.setStatus(Transaction.Status.RECOVERY_ABORTING);
                } else { // END_TRANSACTION
                    entry.transaction.cleanup();
                    entry.transaction.setStatus(Transaction.Status.COMPLETE);
                    transactionTable.remove(transNum);
                    endedTransactions.add(transNum);
                }
            }

            // --- End checkpoint record: reconcile our tables with it.
            if (type == LogType.END_CHECKPOINT) {
                Map<Long, Long> chkptDPT = logRecord.getDirtyPageTable();
                Map<Long, Pair<Transaction.Status, Long>> chkptTxnTable =
                        logRecord.getTransactionTable();

                // The checkpoint DPT is authoritative: copy every entry in.
                for (Map.Entry<Long, Long> e : chkptDPT.entrySet()) {
                    dirtyPageTable.put(e.getKey(), e.getValue());
                }

                for (Map.Entry<Long, Pair<Transaction.Status, Long>> e : chkptTxnTable.entrySet()) {
                    long transNum = e.getKey();
                    if (endedTransactions.contains(transNum)) continue;
                    Transaction.Status chkptStatus = e.getValue().getFirst();
                    long chkptLastLSN = e.getValue().getSecond();

                    if (!transactionTable.containsKey(transNum)) {
                        startTransaction(newTransaction.apply(transNum));
                    }
                    TransactionTableEntry entry = transactionTable.get(transNum);
                    // lastLSN is the max of what we've seen and the checkpoint's.
                    entry.lastLSN = Math.max(entry.lastLSN, chkptLastLSN);

                    // Advance status if the checkpoint's is "more advanced". The
                    // checkpoint records ABORTING but at recovery time that means
                    // RECOVERY_ABORTING.
                    Transaction.Status current = entry.transaction.getStatus();
                    Transaction.Status target = chkptStatus == Transaction.Status.ABORTING
                            ? Transaction.Status.RECOVERY_ABORTING : chkptStatus;
                    if (isMoreAdvancedStatus(current, target)) {
                        entry.transaction.setStatus(target);
                    }
                }
            }
        }

        // Final pass over the ending transaction table.
        for (Map.Entry<Long, TransactionTableEntry> entry : transactionTable.entrySet()) {
            long transNum = entry.getKey();
            TransactionTableEntry tte = entry.getValue();
            Transaction.Status status = tte.transaction.getStatus();
            if (status == Transaction.Status.COMMITTING) {
                // Committed before crash: finish it (end record + cleanup).
                tte.transaction.cleanup();
                tte.transaction.setStatus(Transaction.Status.COMPLETE);
                long endLSN = logManager.appendToLog(
                        new EndTransactionLogRecord(transNum, tte.lastLSN));
                tte.lastLSN = endLSN;
                transactionTable.remove(transNum);
            } else if (status == Transaction.Status.RUNNING) {
                // In-flight at crash: it must be aborted during recovery.
                long abortLSN = logManager.appendToLog(
                        new AbortTransactionLogRecord(transNum, tte.lastLSN));
                tte.lastLSN = abortLSN;
                tte.transaction.setStatus(Transaction.Status.RECOVERY_ABORTING);
            }
            // RECOVERY_ABORTING: nothing to do here.
        }
    }

    /**
     * @return true if `to` is a strictly more-advanced transaction status than
     * `from` (i.e. the transition from -> to is legal during analysis).
     */
    private boolean isMoreAdvancedStatus(Transaction.Status from, Transaction.Status to) {
        return statusRank(to) > statusRank(from);
    }

    private int statusRank(Transaction.Status status) {
        switch (status) {
            case RUNNING: return 0;
            case COMMITTING: return 1;
            case ABORTING: return 1;
            case RECOVERY_ABORTING: return 1;
            case COMPLETE: return 2;
            default: return 0;
        }
    }

    /**
     * This method performs the redo pass of restart recovery.
     *
     * First, determine the starting point for REDO from the dirty page table.
     *
     * Then, scanning from the starting point, if the record is redoable and
     * - partition-related (Alloc/Free/UndoAlloc/UndoFree..Part), always redo it
     * - allocates a page (AllocPage/UndoFreePage), always redo it
     * - modifies a page (Update/UndoUpdate/Free/UndoAlloc....Page) in
     *   the dirty page table with LSN >= recLSN, the page is fetched from disk,
     *   the pageLSN is checked, and the record is redone if needed.
     */
    void restartRedo() {
        // REDO starts at the smallest recLSN in the dirty page table.
        if (dirtyPageTable.isEmpty()) return;
        long startLSN = Collections.min(dirtyPageTable.values());

        Iterator<LogRecord> iter = logManager.scanFrom(startLSN);
        while (iter.hasNext()) {
            LogRecord record = iter.next();
            if (!record.isRedoable()) continue;

            LogType type = record.getType();
            // Partition operations and page allocations are always redone.
            boolean partitionOp = type == LogType.ALLOC_PART || type == LogType.FREE_PART
                    || type == LogType.UNDO_ALLOC_PART || type == LogType.UNDO_FREE_PART;
            boolean allocatesPage = type == LogType.ALLOC_PAGE || type == LogType.UNDO_FREE_PAGE;

            if (partitionOp || allocatesPage) {
                record.redo(this, diskSpaceManager, bufferManager);
                continue;
            }

            // Page-modifying records: redo only if the page is dirty, this
            // record is at or after its recLSN, and the page's on-disk pageLSN
            // is strictly less than this record's LSN.
            boolean modifiesPage = type == LogType.UPDATE_PAGE || type == LogType.UNDO_UPDATE_PAGE
                    || type == LogType.FREE_PAGE || type == LogType.UNDO_ALLOC_PAGE;
            if (modifiesPage && record.getPageNum().isPresent()) {
                long pageNum = record.getPageNum().get();
                if (!dirtyPageTable.containsKey(pageNum)) continue;
                if (record.getLSN() < dirtyPageTable.get(pageNum)) continue;

                Page page = bufferManager.fetchPage(new DummyLockContext(), pageNum);
                try {
                    if (page.getPageLSN() < record.getLSN()) {
                        record.redo(this, diskSpaceManager, bufferManager);
                    }
                } finally {
                    page.unpin();
                }
            }
        }
    }

    /**
     * This method performs the undo pass of restart recovery.

     * First, a priority queue is created sorted on lastLSN of all aborting
     * transactions.
     *
     * Then, always working on the largest LSN in the priority queue until we are done,
     * - if the record is undoable, undo it, and append the appropriate CLR
     * - replace the entry with a new one, using the undoNextLSN if available,
     *   if the prevLSN otherwise.
     * - if the new LSN is 0, clean up the transaction, set the status to complete,
     *   and remove from transaction table.
     */
    void restartUndo() {
        // Max-heap of the lastLSN of every aborting (RECOVERY_ABORTING)
        // transaction. We always undo the largest LSN next.
        PriorityQueue<Long> toUndo = new PriorityQueue<>(Collections.reverseOrder());
        for (TransactionTableEntry entry : transactionTable.values()) {
            if (entry.transaction.getStatus() == Transaction.Status.RECOVERY_ABORTING) {
                toUndo.add(entry.lastLSN);
            }
        }

        while (!toUndo.isEmpty()) {
            long lsn = toUndo.poll();
            LogRecord record = logManager.fetchLogRecord(lsn);
            long transNum = record.getTransNum().get();
            TransactionTableEntry entry = transactionTable.get(transNum);

            if (record.isUndoable()) {
                // Write and apply a CLR to undo this record.
                LogRecord clr = record.undo(entry.lastLSN);
                long clrLSN = logManager.appendToLog(clr);
                entry.lastLSN = clrLSN;
                clr.redo(this, diskSpaceManager, bufferManager);
            }

            // Determine the next record of this transaction to undo: the
            // undoNextLSN if present (skips already-undone records via CLRs),
            // otherwise the prevLSN.
            long nextLSN = record.getUndoNextLSN().orElse(record.getPrevLSN().orElse(0L));
            if (nextLSN == 0) {
                // Transaction fully undone: end it.
                entry.transaction.cleanup();
                entry.transaction.setStatus(Transaction.Status.COMPLETE);
                logManager.appendToLog(new EndTransactionLogRecord(transNum, entry.lastLSN));
                transactionTable.remove(transNum);
            } else {
                toUndo.add(nextLSN);
            }
        }
    }

    /**
     * Removes pages from the DPT that are not dirty in the buffer manager.
     * This is slow and should only be used during recovery.
     */
    void cleanDPT() {
        Set<Long> dirtyPages = new HashSet<>();
        bufferManager.iterPageNums((pageNum, dirty) -> {
            if (dirty) dirtyPages.add(pageNum);
        });
        Map<Long, Long> oldDPT = new HashMap<>(dirtyPageTable);
        dirtyPageTable.clear();
        for (long pageNum : dirtyPages) {
            if (oldDPT.containsKey(pageNum)) {
                dirtyPageTable.put(pageNum, oldDPT.get(pageNum));
            }
        }
    }

    // Helpers /////////////////////////////////////////////////////////////////
    /**
     * Comparator for Pair<A, B> comparing only on the first element (type A),
     * in reverse order.
     */
    private static class PairFirstReverseComparator<A extends Comparable<A>, B> implements
            Comparator<Pair<A, B>> {
        @Override
        public int compare(Pair<A, B> p0, Pair<A, B> p1) {
            return p1.getFirst().compareTo(p0.getFirst());
        }
    }
}
