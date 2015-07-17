package org.apache.bookkeeper;

import java.util.concurrent.locks.ReentrantLock;

import org.apache.bookkeeper.client.LedgerHandle;

class LedgerPrivateData {
	LedgerHandle wlh;  // Write Ledger Handle
	LedgerHandle rlh; // Read Ledger Handle
	volatile long  lId = BKPConstants.NO_ENTRY; // LedgerId
	volatile long tEntryId = BKPConstants.NO_ENTRY;   // Trailer/Last entryId of the ledger
	volatile long aEntryId = BKPConstants.NO_ENTRY;                         // Max entryId that is allocated/granted to the client
	volatile long wEntryId = BKPConstants.NO_ENTRY;   // Latest committed entryId to BK.
	private final ReentrantLock lpLock = new ReentrantLock();
	
	public LedgerHandle getWriteLedgerHandle() {
		return wlh;
	}
	public synchronized void  setWriteLedgerHandle(LedgerHandle lh) {
		this.wlh = lh;
		if (lh != null && this.lId == BKPConstants.NO_ENTRY)
			this.lId = lh.getId();
	}
	public long getLedgerId() {
		return lId;
	}
	public LedgerHandle getReadLedgerHandle() {
		return rlh;
	}
	public synchronized void setReadLedgerHandle(LedgerHandle rlh) {
		this.rlh = rlh;
		if (rlh != null && this.lId == BKPConstants.NO_ENTRY)
			this.lId = rlh.getId();
	}
	public long getTrailerId() {
		return tEntryId;
	}
	public void setTrailerId(long trailerId) {
		this.tEntryId = trailerId;
	}
	
	public void setLastWriteEntryId(long entryId) {
		this.wEntryId = entryId;
	}
	
	public long getLastWriteEntryId() {
		return this.wEntryId;
	}
	
	public void lockLedger() {
		lpLock.lock();
	}
	
	public void unlockLedger() {
		try {
			lpLock.unlock();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public synchronized long allocateEntryId() {

		if (aEntryId == BKPConstants.NO_ENTRY) // First one
			aEntryId = 0;
		else
			aEntryId++;

		return aEntryId;
	}
	
	public synchronized long getAllocedEntryId() {
		return aEntryId;
	}
}
