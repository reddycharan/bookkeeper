package org.apache.bookkeeper;

import java.util.concurrent.ConcurrentHashMap;

import org.apache.bookkeeper.client.LedgerHandle;

public class BKExtentLedgerMap {
	private ConcurrentHashMap<String, LedgerPrivateData> extentToLedgerMap = new ConcurrentHashMap<String, LedgerPrivateData>();
	

	public LedgerPrivateData getLedgerPrivate(String extentId) {
		return extentToLedgerMap.get(extentId);
	}
	
	public boolean extentExists(String extentID) {
		return extentToLedgerMap.containsKey(extentID);
	}
	
	public void deleteLedgerPrivate(String extentId) {
		extentToLedgerMap.remove(extentId);
	}
	
	public void createLedgerMap(String extentId, LedgerHandle lh) {
		
		LedgerPrivateData lpd = new LedgerPrivateData();
		
		lpd.setWriteLedgerHandle(lh);
		// Just opened, no trailer.
		lpd.setTrailerId(BKPConstants.NO_ENTRY);
		extentToLedgerMap.put(extentId, lpd);
	}
	
	public void deleteAllLedgerHandles() {
		// TODO: Delete all LedgerHandles too
		extentToLedgerMap.clear();
	}
}

