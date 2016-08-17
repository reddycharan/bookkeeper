package org.apache.bookkeeper;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.LedgerHandle;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.bookkeeper.BKProxyWorker.OpStatEntry;

class LedgerPrivateData {

    public enum LedgerHandleType {
        WRITE,
        NONRECOVERYREAD,
        RECOVERYREAD,
    };

    private final ConcurrentMap<Integer, LedgerAsyncWriteStatus> asyncWriteStatus;

    private final LedgerHandle lh;
    private final LedgerHandleType lhType;

    private LedgerPrivateData(LedgerHandle lh, LedgerHandleType type) {
        this.lh = lh;
        this.lhType = type;
        if (this.lhType == LedgerHandleType.WRITE) {
            this.asyncWriteStatus = new ConcurrentHashMap<Integer, LedgerAsyncWriteStatus>();
        } else {
            this.asyncWriteStatus = null;
        }
    }

    public static LedgerPrivateData buildWriteHandle(LedgerHandle lh) {
        return new LedgerPrivateData(lh, LedgerHandleType.WRITE);
    }

    public static LedgerPrivateData buildRecoveryReadHandle(LedgerHandle lh) {
        return new LedgerPrivateData(lh, LedgerHandleType.RECOVERYREAD);
    }

    public static LedgerPrivateData buildNonRecoveryReadHandle(LedgerHandle lh) {
        return new LedgerPrivateData(lh, LedgerHandleType.NONRECOVERYREAD);
    }

    public LedgerHandle getLedgerHandle() {
        return lh;
    }

    public LedgerHandleType getLedgerHandleType() {
        return this.lhType;
    }

    public LedgerAsyncWriteStatus getLedgerFragmentAsyncWriteStatus(int fragmentId) {
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
        return !asyncWriteStatus.isEmpty();
    }

    public LedgerAsyncWriteStatus createLedgerAsyncWriteStatus(int fragmentId, long entryId, Queue<OpStatEntry> asyncStatQueue) {
        LedgerAsyncWriteStatus laws = new LedgerAsyncWriteStatus(fragmentId, entryId, asyncStatQueue);
        asyncWriteStatus.putIfAbsent(fragmentId, laws);
        return asyncWriteStatus.get(fragmentId);
    }

    public void closeLedgerHandle() throws InterruptedException, BKException {
        if (this.lh != null){
            this.lh.close();
        }
    }
}
