package org.apache.bookkeeper;

import org.apache.bookkeeper.client.LedgerHandle;

class LedgerPrivateData {
    LedgerHandle wlh; // Write Ledger Handle
    LedgerHandle rlh; // Read Ledger Handle
    volatile long tEntryId = BKPConstants.NO_ENTRY; // Trailer/Last entryId of the ledger

    public LedgerHandle getWriteLedgerHandle() {
        return wlh;
    }

    public synchronized void setWriteLedgerHandle(LedgerHandle lh) {
        this.wlh = lh;
    }

    public LedgerHandle getReadLedgerHandle() {
        return rlh;
    }

    public synchronized void setReadLedgerHandle(LedgerHandle rlh) {
        this.rlh = rlh;
    }

    public long getTrailerId() {
        return tEntryId;
    }

    public void setTrailerId(long trailerId) {
        assert (this.tEntryId == BKPConstants.NO_ENTRY);
        this.tEntryId = trailerId;
    }

}
