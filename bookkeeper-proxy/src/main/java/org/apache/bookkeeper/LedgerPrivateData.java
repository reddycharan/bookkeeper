package org.apache.bookkeeper;

import org.apache.bookkeeper.client.LedgerHandle;

class LedgerPrivateData {
    LedgerHandle wlh = null; // Write Ledger Handle
    LedgerHandle rlh = null; // Read Ledger Handle
    LedgerHandle rrlh = null; // Recovery Read Ledger Handle.
    volatile long tEntryId = BKPConstants.NO_ENTRY; // Trailer/Last entryId of the ledger

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

    public long getTrailerId() {
        return tEntryId;
    }

    public void setTrailerId(long trailerId) {
        assert (this.tEntryId == BKPConstants.NO_ENTRY);
        this.tEntryId = trailerId;
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
