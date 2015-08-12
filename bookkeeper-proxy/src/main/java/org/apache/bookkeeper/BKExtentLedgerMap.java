package org.apache.bookkeeper;

import java.util.Hashtable;
import java.util.Set;

import org.apache.bookkeeper.client.LedgerHandle;

public class BKExtentLedgerMap {
	private Hashtable<String, LedgerPrivateData> extentToLedgerMap = new Hashtable<String, LedgerPrivateData>();
	

	public LedgerPrivateData getLedgerPrivate(String extentId) {
		return extentToLedgerMap.get(extentId);
	}

	public LedgerHandle getLedgerHandle(String extentId) {
		return extentToLedgerMap.get(extentId).getLedgerHandle();
	}
	
	public boolean extentExists(String extentID) {
		if (extentToLedgerMap.get(extentID)  != null)
			return true;
		
		return false;
	}
	
	public void deleteLedgerPrivate(String extentId) {
		extentToLedgerMap.remove(extentId);
	}
	
	public void createLedger(String extentId, LedgerHandle lh) {
		
		LedgerPrivateData lpd = new LedgerPrivateData();
		
		lpd.setLedgerHandle(lh);
		// Just opened, no trailer.
		lpd.setTrailerId(BKPConstants.NO_ENTRY);
		extentToLedgerMap.put(extentId, lpd);
	}
	
	public void deleteAllLedgerHandles() {
		// TODO: Delete all LedgerHandles too
		extentToLedgerMap.clear();
	}
}

