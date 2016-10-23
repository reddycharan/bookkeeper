package org.apache.bookkeeper;

import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.LedgerPrivateData.LedgerHandleType;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BKException.Code;
import org.apache.bookkeeper.client.LedgerHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

public class BKExtentLedgerMap {
    private final static Logger LOG = LoggerFactory.getLogger(BKExtentLedgerMap.class);

    private final long readHandleCacheLen;
    private final long readHandleTTL;

    private final ConcurrentHashMap<BKExtentId, LedgerPrivateData> writeLhMap;
    private final Cache<BKExtentId, LedgerPrivateData> readLhLru;

    // Removal listener should only close the handle and must not modify the 
    // value of any member variable of LedgerPrivateData.
    private RemovalListener<BKExtentId, LedgerPrivateData> removalListener = 
            new RemovalListener<BKExtentId, LedgerPrivateData>() {
        @Override
        public void onRemoval(RemovalNotification<BKExtentId, LedgerPrivateData> entry) {
            LOG.debug("Closing read handle for extentId {} due to reason {}",
                    entry.getKey(), entry.getCause().name());
            LedgerPrivateData lpd = entry.getValue();
            // Guava ensures that values can never be null.
            LedgerHandleType lhType = lpd.getLedgerHandleType();
            if (lhType == LedgerHandleType.WRITE) {
                LOG.error("Found {} type of handle in readMap", lhType.name());
                return;
            }
            if (lpd.getLedgerHandle() == null) {
                LOG.error("The ledger handle should not be null!");
                return;
            }

            try {
              lpd.closeLedgerHandle();
            } catch (InterruptedException | BKException e) {
              LOG.error("Exception while closing handle for extentId {} due to reason {} ",
                  new Object[] {entry.getKey(), entry.getCause().name(), e});
            }
        }
    };

    public BKExtentLedgerMap(long readHandleCacheLen, long readHandleTTL) {
        this.readHandleCacheLen = readHandleCacheLen;
        this.readHandleTTL = readHandleTTL;

        this.writeLhMap = new ConcurrentHashMap<BKExtentId, LedgerPrivateData>();

        if (this.readHandleTTL == BKPConstants.INFINITY) {
            this.readLhLru = CacheBuilder.newBuilder()
                    .maximumSize(this.readHandleCacheLen)
                    .removalListener(this.removalListener)
                    .build();
        } else {
            this.readLhLru = CacheBuilder.newBuilder()
                    .maximumSize(this.readHandleCacheLen)
                    .expireAfterAccess(this.readHandleTTL, TimeUnit.MILLISECONDS)
                    .removalListener(this.removalListener)
                    .build();
        }

        LOG.info("Created read-map with cache-len = {} entries and ttl = {}ms",
                this.readHandleCacheLen,
                (this.readHandleTTL == BKPConstants.INFINITY ? "INFINITY " : String.valueOf(this.readHandleTTL)));
    }

    public static class BKExtentLedgerMapBuilder {
        private long readHandleCacheLen = -999;
        private long readHandleTTL = -999;

        public BKExtentLedgerMap build() {
            if ((readHandleCacheLen < 0) || (readHandleTTL < -1)) {  // -1 defaults to unlimited for TTL
                throw new IllegalArgumentException("All parameters not set correctly for BKExtentLedgerMap!");
            }

            return new BKExtentLedgerMap(this.readHandleCacheLen, this.readHandleTTL);
        }

        public BKExtentLedgerMapBuilder setReadHandleCacheLen(long readHandleCacheLen) {
            this.readHandleCacheLen = readHandleCacheLen;
            return this;
        }

        public BKExtentLedgerMapBuilder setReadHandleTTL(long readHandleTTL) {
            this.readHandleTTL = readHandleTTL;
            return this;
        }
    }

    public static BKExtentLedgerMapBuilder newBuilder() {
        return new BKExtentLedgerMapBuilder();
    }

    public Set<BKExtentId> getAllExtentIds() {
        Set<BKExtentId> idSet = new HashSet<BKExtentId>();
        for (Entry<BKExtentId, LedgerPrivateData> entry: writeLhMap.entrySet()) {
            idSet.add(entry.getKey());
        }
        for (Entry<BKExtentId, LedgerPrivateData> entry: readLhLru.asMap().entrySet()) {
            idSet.add(entry.getKey());
        }

        return idSet;
    }

    public void addWriteLedger(BKExtentId extentId, LedgerPrivateData lpd) throws BKException {
        if (writeLhMap.putIfAbsent(extentId, lpd) != null) {
            // there must be no existing entry in writeLhMap
            throw BKException.create(Code.UnexpectedConditionException);
        }
    }

    private LedgerPrivateData getSpecificLedgerPrivateData(BKExtentId extentId, LedgerHandleType type) {
        switch(type) {
        case WRITE:
            return writeLhMap.get(extentId);

        case RECOVERYREAD:
        case NONRECOVERYREAD:
            LedgerPrivateData lpd = readLhLru.getIfPresent(extentId);
            if ((lpd == null) || (lpd.getLedgerHandleType() != type)) {
                return null;
            }
            return lpd;
        default:
            return null;
        }
    }

    public LedgerHandle getSpecifiedLedgerHandle(BKExtentId extentId, LedgerHandleType type) {
        LedgerPrivateData lpd = getSpecificLedgerPrivateData(extentId, type);
        if (lpd == null) {
            return null;
        }
        return lpd.getLedgerHandle();
    }

    public LedgerPrivateData getWriteLedgerPrivateData(BKExtentId extentId) {
        return getSpecificLedgerPrivateData(extentId, LedgerHandleType.WRITE);
    }

    public LedgerHandle getWriteLedgerHandle(BKExtentId extentId) {
        return getSpecifiedLedgerHandle(extentId, LedgerHandleType.WRITE);
    }

    public void closeWriteLedgerHandle(BKExtentId extentId) throws InterruptedException, BKException {
        LedgerHandle lh = getSpecifiedLedgerHandle(extentId, LedgerHandleType.WRITE);
        if (lh == null) {
            LOG.warn("Null handle obtained for extent {} while closing", extentId);
            return;
        }
        LOG.debug("Closing write ledger handle for extent {}", extentId);
        lh.close();
    }

    public void addNonRecoveryReadLedgerPrivateData(BKExtentId extentId, LedgerPrivateData lpd) {
        LedgerHandleType lhType = lpd.getLedgerHandleType();
        if (lhType != LedgerHandleType.NONRECOVERYREAD) {
            LOG.error("Attempt to add invalid type of handle {} as NonRecoveryRead", lhType.name());
            return;
        }
        readLhLru.put(extentId, lpd);
    }

    public LedgerPrivateData getRecoveryReadLedgerPrivateData(BKExtentId extentId) {
        return getSpecificLedgerPrivateData(extentId, LedgerHandleType.RECOVERYREAD);
    }

    public LedgerHandle getRecoveryReadLedgerHandle(BKExtentId extentId) {
        return getSpecifiedLedgerHandle(extentId, LedgerHandleType.RECOVERYREAD);
    }

    public void addRecoveryReadLedgerPrivateData(BKExtentId extentId, LedgerPrivateData lpd) {
        LedgerHandleType lhType = lpd.getLedgerHandleType();
        if (lhType != LedgerHandleType.RECOVERYREAD) {
            LOG.error("Attempt to add invalid type of handle {} as RecoveryRead", lhType.name());
            return;
        }
        readLhLru.put(extentId, lpd);
    }

    public LedgerPrivateData getAnyLedger(BKExtentId extentId) {
        LedgerPrivateData lpd = writeLhMap.get(extentId);
        if (lpd == null) {
            lpd = readLhLru.getIfPresent(extentId);
        }
        return lpd;
    }

    public LedgerHandle getAnyLedgerHandle(BKExtentId extentId) {
        LedgerPrivateData lpd = getAnyLedger(extentId);
        if (lpd == null) {
            return null;
        }

        LedgerHandle lh = lpd.getLedgerHandle();
        if (lh == null) {
            LOG.error("Found null ledger handle for extent {}", extentId);
        }
        return lh;
    }

    public boolean anyLedgerExists(BKExtentId extentId) {
        // There cannot be an lpd without a handle, so just check for ledger
        return (getAnyLedger(extentId) != null);
    }

    public void removeReadLedger(BKExtentId extentId) {
        LOG.debug("Closing read ledger handle for extentId {} if present", extentId);
        readLhLru.invalidate(extentId);
    }

    public LedgerPrivateData removeWriteLedger(BKExtentId extentId) throws InterruptedException, BKException {
        LedgerHandle lh = this.getWriteLedgerHandle(extentId);
        if (lh == null) {
            return null;
        }
        lh.close();
        return writeLhMap.remove(extentId);
    }

    public void removeFromLedgerMap(BKExtentId extentId) throws InterruptedException, BKException {
        // Only one of the caches will contain the extent, so try and
        // remove from both caches.
        LedgerPrivateData writeLPD = removeWriteLedger(extentId);
        if (writeLPD == null) {
            removeReadLedger(extentId);
        }
    }

}
