package org.apache.bookkeeper;

import java.util.concurrent.ConcurrentHashMap;

public class BKExtentLedgerMap {
    private ConcurrentHashMap<BKExtentId, LedgerPrivateData> extentToLedgerMap = new ConcurrentHashMap<BKExtentId, LedgerPrivateData>();

    public LedgerPrivateData getLedgerPrivate(BKExtentId extentId) {
        return extentToLedgerMap.get(extentId);
    }

    public boolean extentMapExists(BKExtentId extentId) {
        return extentToLedgerMap.containsKey(extentId);
    }

    public void deleteLedgerPrivate(BKExtentId extentId) {
        extentToLedgerMap.remove(extentId);
    }

    public LedgerPrivateData createLedgerMap(BKExtentId extentId) {
        LedgerPrivateData lpd = new LedgerPrivateData();
        // Just opened, no trailer.
        lpd.setTrailerId(BKPConstants.NO_ENTRY);
        extentToLedgerMap.putIfAbsent(extentId.copy(), lpd);
        return extentToLedgerMap.get(extentId);
    }

    public BKExtentId[] getAllExtentIds() {
        return extentToLedgerMap.keySet().toArray(new BKExtentId[extentToLedgerMap.keySet().size()]);
    }

    public void deleteAllLedgerHandles() {
        extentToLedgerMap.clear();
    }
}
