package org.apache.bookkeeper;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.bookkeeper.client.LedgerHandle;

class LedgerPrivateData {
    LedgerHandle wlh = null; // Write Ledger Handle
    LedgerHandle rlh = null; // Read Ledger Handle
    LedgerHandle rrlh = null; // Recovery Read Ledger Handle.
    private Lock ledgerRecoveryOpenLock;
    private ConcurrentMap<Integer, LedgerAsyncWriteStatus> asyncWriteStatus;

    public LedgerAsyncWriteStatus getLedgerAsyncWriteStatus(int fragmentId) {
        return asyncWriteStatus.get(fragmentId);
    }

    public void deleteLedgerAsyncWriteStatus(int fragmentId) {
        asyncWriteStatus.remove(fragmentId);
    }

    // Return TRUE if writes are waiting either for
    // Bookie response or writes that were not called
    // LedgerAsyncWriteStatusReq from SDB.
    // If no pending writes, return FALSE.
    public boolean anyAsyncWritesPending() {
        if (asyncWriteStatus.isEmpty()) {
            return false;
        }
        return true;
    }
    public LedgerAsyncWriteStatus createLedgerAsyncWriteStatus(int fragmentId, long entryId) {
        LedgerAsyncWriteStatus laws = new LedgerAsyncWriteStatus(fragmentId, entryId);
        asyncWriteStatus.putIfAbsent(fragmentId, laws);
        return asyncWriteStatus.get(fragmentId);
    }

    LedgerPrivateData() {
        this.ledgerRecoveryOpenLock = new ReentrantLock();
        this.asyncWriteStatus = new ConcurrentHashMap<Integer, LedgerAsyncWriteStatus>();

    }

    public void acquireLedgerRecoveryOpenLock(){
        ledgerRecoveryOpenLock.lock();
    }
    
    public void releaseLedgerRecoveryOpenLock(){
        ledgerRecoveryOpenLock.unlock();
    }

    public LedgerHandle getWriteLedgerHandle() {
        return wlh;
    }

    public synchronized void setWriteLedgerHandle(LedgerHandle lh) {
        this.wlh = lh;
    }

    public LedgerHandle getRecoveryReadLedgerHandle() {
        return rrlh;
    }

    public synchronized void setRecoveryReadLedgerHandle(LedgerHandle rrlh) {
        this.rrlh = rrlh;
    }

    public LedgerHandle getNonRecoveryReadLedgerHandle() {
        return rlh;
    }

    public synchronized void setNonRecoveryReadLedgerHandle(LedgerHandle rlh) {
        this.rlh = rlh;
    }

    // This returns any of the existing ledger handles in the order of
    // priority. This can be used for read/stat calls.
    public LedgerHandle getAnyLedgerHandle() {
        // Write ledger handle
        if (wlh != null)
            return wlh;

        // recovery read ledger handle
        if (rrlh != null)
            return rrlh;

        // non-recovery read ledger handle
        if (rlh != null)
            return rlh;

        // No ledger handle available.
        return null;
    }

}
