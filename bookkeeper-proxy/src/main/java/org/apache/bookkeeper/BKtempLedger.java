package org.apache.bookkeeper;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Set;

public class BKtempLedger {
	Hashtable<String, ByteBuffer> tempLedger = new Hashtable<String, ByteBuffer>();
	List<String> ExtentList = new ArrayList<String>();

	public Hashtable<String, ByteBuffer> getTempLedger() {
		return tempLedger;
	}

	public void setTempLedger(Hashtable<String, ByteBuffer> tempLedger) {
		this.tempLedger = tempLedger;
	}
	
	// TODO: Change this to return actual stat information
	// For now, returns true if extent exists, false otherwise.
	
	public boolean LedgerStat(String extentId) {
		
		if (ExtentList.contains(extentId))
			return true;
		
		return false;
	}
	// We have one hashtable for all ledgers. So if the global tempLedger
	// is present, we are all set.
	public byte LedgerCreate(String extentId) {
		
		if (tempLedger == null)
			tempLedger = new Hashtable<String, ByteBuffer>();
		
		ExtentList.add(extentId);

		return BKPConstants.SF_OK;
	}
	
	// Find and delete all the entries belong to the given exetntId
	public byte LedgerDelete(String extentId) {
		
		Set<String> keys = tempLedger.keySet();
		
		for(String key: keys) {
			if (key.endsWith(extentId))
				tempLedger.remove(key);
		}
		
		return BKPConstants.SF_OK;
	}
	
	public void LedgerDeleteAll() {
		ExtentList.clear();
		tempLedger.clear();
	}
	
	public byte LedgerPutEntry(String ExtentId, int entryId, ByteBuffer ledgerEntry) {
		String uniqueEntry = entryId + "/" + ExtentId;
		System.out.println("Unique ID: " + uniqueEntry);
		
		tempLedger.put(uniqueEntry, ledgerEntry);

		return BKPConstants.SF_OK;
	
	}
	
	public ByteBuffer LedgerGetEntry(String ExtentId, int entryId, int size) {
		String uniqueEntry = entryId + "/" + ExtentId;
		System.out.println("Unique ID: " + uniqueEntry);
		
		return tempLedger.get(uniqueEntry);

	}

}
