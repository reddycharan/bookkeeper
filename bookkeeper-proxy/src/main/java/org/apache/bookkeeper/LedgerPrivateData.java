package org.apache.bookkeeper;

import java.util.concurrent.locks.ReentrantLock;

import org.apache.bookkeeper.client.LedgerHandle;

class LedgerPrivateData {
	LedgerHandle lh;
	long tEntryId = BKPConstants.NO_ENTRY;   // Trailer/Last entryId of the ledger
	long aEntryId = BKPConstants.NO_ENTRY;                         // Max entryId that is allocated/granted to the client
	long wEntryId = BKPConstants.NO_ENTRY;   // Latest committed entryId to BK.
	private final ReentrantLock lpLock = new ReentrantLock();
	
	public LedgerHandle getLedgerHandle() {
		return lh;
	}
	public void setLedgerHandle(LedgerHandle lh) {
		this.lh = lh;
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
	
	public long allocateEntryId() {
		long eId;
		
		lockLedger();
		
		if (aEntryId == BKPConstants.NO_ENTRY) // First one
			aEntryId = 0;
		else
			aEntryId++;
		eId = aEntryId;
		
		unlockLedger();
		
		return eId;
	}
	
	public long getAllocedEntryId() {
		return aEntryId;
	}
}