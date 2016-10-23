package org.apache.bookkeeper;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Enumeration;
import java.util.Queue;

import org.apache.bookkeeper.BKProxyWorker.OpStatEntry;
import org.apache.bookkeeper.client.AsyncCallback.AddCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BKException.Code;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.conf.BookKeeperProxyConfiguration;
import org.apache.bookkeeper.stats.StatsLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BKSfdcClient {
    private final static Logger LOG = LoggerFactory.getLogger(BKSfdcClient.class);
    private final BookKeeperProxyConfiguration bkpConfig;
    private final BKByteBufferPool bufPool;
    BKExtentLedgerMap elm = null;
    long ledgerId = LedgerHandle.INVALID_ENTRY_ID;
    BookKeeper bk = null;
    LedgerEntry ledgerEntry = null;
    Object ledgerObj = null;
    ByteBuffer cByteBuffer;
    private int ensembleSize;
    private int writeQuorumSize;
    private int ackQuorumSize;
    private String password;
    private DigestType digestType;

    public BKSfdcClient(BookKeeperProxyConfiguration bkpConfig, BookKeeper bk, BKExtentLedgerMap elm,
            BKByteBufferPool bufPool, StatsLogger statsLogger) {
        this.bkpConfig = bkpConfig;
        this.bk = bk;
        this.elm = elm;
        this.bufPool = bufPool;
        initBookkeeperConfiguration();
    }

    private void initBookkeeperConfiguration() {
        cByteBuffer = ByteBuffer.allocate(bkpConfig.getMaxFragSize());
        ensembleSize = bkpConfig.getEnsembleSize();
        writeQuorumSize = bkpConfig.getWriteQuorumSize();
        ackQuorumSize = bkpConfig.getAckQuorumSize();
        password = bkpConfig.getPassword();
        digestType = bkpConfig.getDigestType();
    }

    // Synchronization not needed here because Bookkeeper guarantees only
    // one ledger will be created.
    public void ledgerCreate(BKExtentId extentId) throws BKException, InterruptedException {
        if (elm.anyLedgerExists(extentId)) {
            throw BKException.create(Code.LedgerExistException);
        }

        LedgerHandle lh = bk.createLedgerAdv(extentId.asLong(), ensembleSize, writeQuorumSize, ackQuorumSize,
                digestType, password.getBytes(), null);
        final LedgerPrivateData lpd = LedgerPrivateData.buildWriteHandle(lh, bufPool);
        elm.addWriteLedger(extentId, lpd);
    }

    // creates new extent ID and creates ledger for it
    public synchronized BKExtentId ledgerCreate() throws BKException, InterruptedException {
        LedgerHandle lh = bk.createLedgerAdv(ensembleSize, writeQuorumSize, ackQuorumSize,
                digestType, password.getBytes());

        final BKExtentId extentId = new BKExtentIdByteArray(lh.getId());
        final LedgerPrivateData lpd = LedgerPrivateData.buildWriteHandle(lh, bufPool);
        elm.addWriteLedger(extentId, lpd);

        return extentId;
    }

    /*
     * Opens an inactive ledger. i.e no more writes are going in. If this mode
     * is attempted on an active ledger, it will stop accepting any more writes
     * after this operation.
     */
    public LedgerHandle ledgerRecoveryOpenRead(BKExtentId extentId) throws BKException, InterruptedException {

        // fail if there is any write ledger
        if (elm.getWriteLedgerHandle(extentId) != null) {
            LOG.warn("Recovery-read when a Write ledger handle exists for extentId {}",
                extentId);
            elm.removeWriteLedger(extentId);
        }

        LedgerHandle lh = elm.getRecoveryReadLedgerHandle(extentId);
        if (lh != null) {
            return lh;
        }

        lh = bk.openLedger(extentId.asLong(), digestType, password.getBytes());
        final LedgerPrivateData lpd = LedgerPrivateData.buildRecoveryReadHandle(lh, bufPool);
        elm.addRecoveryReadLedgerPrivateData(extentId, lpd);
        return lh;
    }

    /*
     * Opens ledger while it is still actively adding(writing) entries. In this
     * mode, ledger is changing hence one may not get the latest and greatest
     * state and entries of the ledger.
     */
    public LedgerHandle ledgerNonRecoveryOpenRead(BKExtentId extentId) throws BKException, InterruptedException {

        // Any handle can be used for non-recovery read
        LedgerHandle lh = elm.getAnyLedgerHandle(extentId);
        if (lh != null) {
            return lh;
        }

        lh = bk.openLedgerNoRecovery(extentId.asLong(), digestType, password.getBytes());
        final LedgerPrivateData lpd = LedgerPrivateData.buildNonRecoveryReadHandle(lh, bufPool);
        elm.addNonRecoveryReadLedgerPrivateData(extentId, lpd);

        return lh;
    }

    public LedgerStat ledgerStat(BKExtentId extentId) throws BKException, InterruptedException {
        LedgerHandle lh = elm.getAnyLedgerHandle(extentId);
        if (lh != null) {
            return new LedgerStat(lh.getLength(), lh.getCtime());
        }

        // No ledger cached locally.
        // Just open/get size and close the extent.
        lh = bk.openLedgerNoRecovery(extentId.asLong(), digestType, password.getBytes());
        long ledgerLength = lh.getLength();
        long ledgerCtime = lh.getCtime();
        lh.close();

        return new LedgerStat(ledgerLength, ledgerCtime);
    }

    // W-3049495: SDb expects int fragment ids 
    private int lacToInt(long lac) {
        if(lac > Integer.MAX_VALUE || lac < 0) {
            throw new IllegalStateException("LAC is out of the range expected by SDB " + lac);
        }
        return (int) lac;
    }
    
    public int ledgerLac(BKExtentId extentId) throws BKException, InterruptedException {
        long lac = -1;

        // Check if we have write ledger handle open.
        LedgerHandle lh = elm.getWriteLedgerHandle(extentId);
        if (lh != null) {
            lac = lh.getLastAddConfirmed();
            return lacToInt(lac);
        }

        // No ledger cached locally.
        // Just open/get size and close the extent.
        lh = bk.openLedgerNoRecovery(extentId.asLong(), digestType, password.getBytes());
        if (lh != null) {
            lac = lh.getLastAddConfirmed();
            lh.close();
        } else {
            LOG.error("Ledger for extentId: {} does not exist.", extentId.asLong());
        }
        return lacToInt(lac);
    }
    
    public void ledgerDeleteAll() throws BKException, InterruptedException {
        for (BKExtentId extentId: elm.getAllExtentIds()) {
            this.ledgerDelete(extentId);
        }
    }

    public Iterable<Long> ledgerList() throws IOException {
        BookKeeperAdmin admin = new BookKeeperAdmin(bk);
        return admin.listLedgers();
    }

    public void ledgerWriteClose(BKExtentId extentId) throws BKException, InterruptedException {
        LOG.info("Closing extentId: {} when it's LAC: {}",
                extentId, elm.getWriteLedgerHandle(extentId).getLastAddConfirmed());
        // Closes and removes write ledger handle from cache.
        elm.removeWriteLedger(extentId);
    }

    public void ledgerReadClose(BKExtentId extentId) throws BKException, InterruptedException {
        // Closes and removes read ledger handle from cache.
        elm.removeReadLedger(extentId);
        return;
    }

    public void ledgerDelete(BKExtentId extentId) throws InterruptedException, BKException {
        // write ledger handles need to be closed before deletion
        if (elm.getWriteLedgerPrivateData(extentId) != null) {
            try {
                elm.closeWriteLedgerHandle(extentId);
            } catch (InterruptedException | BKException e) {
                LOG.warn("Exception when trying to close write-handle during delete for extentId {}", extentId, e);
            }
        }
        bk.deleteLedger(extentId.asLong());

        // Remove the handle from the cache only if ledger deletion is successful.
        elm.removeFromLedgerMap(extentId);
    }

    public LedgerAsyncWriteStatus ledgerAsyncWriteStatus(BKExtentId extentId, int fragmentId, int timeout) throws BKException, InterruptedException {
        byte errorCode = BKPConstants.SF_OK;

        LedgerPrivateData lpd = elm.getWriteLedgerPrivateData(extentId);
        if (lpd == null) {
            LOG.error("LedgerPrivateData is missing: Attempt to get async write status extentId : {} and fragmentId: {}",
                    extentId, fragmentId);
            throw BKException.create(Code.IllegalOpException);
        }

        LedgerAsyncWriteStatus laws = lpd.getLedgerFragmentAsyncWriteStatus(fragmentId);
        if (laws == null) {
            LOG.error("No LedgerAsyncWriteStatus : Attempt to get async write status extentId : {} and fragmentId: {}",
                    extentId, fragmentId);
            throw BKException.create(Code.IllegalOpException);
        }

        errorCode = laws.getResult();
        if ((timeout != 0) && (errorCode == BKPConstants.SF_ErrorInProgress)) {
            laws.waitForResult(timeout);
            errorCode = laws.getResult();
        }

        if (errorCode != BKPConstants.SF_ErrorInProgress) {
            // IO Finished.
            long lac = elm.getWriteLedgerHandle(extentId).getLastAddConfirmed();
            lpd.deleteLedgerAsyncWriteStatus(fragmentId);
            if (errorCode == BKPConstants.SF_OK) {
                // Success; Do some sanity check.
                long actualEntryId = laws.getActualEntryId();
                long expectedEntryId = laws.getExpectedEntryId();
                if (actualEntryId != expectedEntryId) {
                    LOG.error("AsyncWrite failed. Expecting entryId: {} but bk returned entryId: {} on Ledger: {} with LAC: {}",
                            new Object [] {expectedEntryId, actualEntryId, extentId, lac});
                    throw BKException.create(Code.UnexpectedConditionException);
                }
            } else {
                // IO Failed
                LOG.error("AsyncWrite failed. Extent: {} Fragment: {} LAC: {} error: {}",
                        new Object [] {extentId, fragmentId, lac, errorCode});
            }
        }
        return laws;
    }

    public void ledgerPutEntry(BKExtentId extentId, int fragmentId, ByteBuffer bdata,
            Queue<OpStatEntry> asyncStatQueue) throws BKException, InterruptedException {

        long entryId;
        LedgerHandle lh = null;

        final LedgerPrivateData lpd = elm.getWriteLedgerPrivateData(extentId);
        if (lpd != null) {
            lh = lpd.getLedgerHandle();
        }

        if ((lpd == null) || (lh == null)) {
            LOG.error("Attempt to write to extentId : {} with no write ledger handle", extentId);
            throw BKException.create(Code.IllegalOpException);
        }

        // API Requirement:
        // Need to map first and last fragmentIds between SDB and BK.
        //
        // 1. SDB treats FragmentId 0 as the last entry.
        // For BK it is really the last entry (highest entryId of the ledger).
        // 2. FragmentId 1 is the first entry for SDB, but
        // entryId 1 is the second entry for BK as it starts entryId with 0.
        //
        // Rules:
        // 1. FragmentId = entryId + 1; first entryId is 0 and first FragmentId is 1
        // 2. TrailerId(FragmentId 0) = Last entryId of the Ledger.

        // entryId and FragmentId are off by one.
        // BK starts at entryID 0 and Store assumes it to be 1.
        long tmpEntryId;
        if (fragmentId != 0) {
            tmpEntryId = fragmentId - 1;
        } else {
            // SDB guarantees that all entries were acked/responded back
            // before sending fragment 0. So no need to worry about any
            // race condition here.
            if (lpd.anyAsyncWritesPending()) {
                // SDB trying to seal/close extent before it got responses
                // back from sfstore or before it even called LedgerAsyncWriteStatusReq
                // on pending async writes. As per contract it must not happen.
                // Flag a warning, and SDB may expect errors of subsequent writes.
                LOG.error("SDB is trying to close extent: {} with pending writes", extentId);
            }
            tmpEntryId = lh.getLastAddConfirmed() + 1;
        }

        if (asyncStatQueue != null) {
            LedgerAsyncWriteStatus laws;
            ProxyAddCallback callback = new ProxyAddCallback();
            // Create LedgerAsyncWaitStatus object.
            laws = lpd.createLedgerAsyncWriteStatus(fragmentId, tmpEntryId, bdata, asyncStatQueue);
            lh.asyncAddEntry(tmpEntryId, bdata.array(), 0, bdata.limit(), callback, laws);
        } else {
            // Try to catch out-of-sequence addEntry.
            // Condition below is to check and reduce number of log messages for sequential addEntrys through this worker thread.
            // If multiple threads are adding entries simultaneously, we may log the debug message anyway.
            // This is temporary debug message to catch out of order writes.
            long tmpLac = lh.getLastAddConfirmed();
            if ((tmpLac + 1) != tmpEntryId) {
                LOG.info("Sending non-sequential addEntry. LedgerId:{} currentEntryId:{}, LAC:{}",
                    new Object[] {extentId, tmpEntryId, tmpLac});
            }
            entryId = lh.addEntry(tmpEntryId, bdata.array(), 0, bdata.limit());
            if (entryId != tmpEntryId) {
                LOG.info("expecting entryId: " + tmpEntryId + "but bk returned entryId: " + entryId +
                        " On ledger: " + extentId);
                throw BKException.create(Code.UnexpectedConditionException);
            }
        }

        // Handle Trailer
        if (fragmentId == 0) {
            // Close the ledger
            ledgerWriteClose(extentId);
        }
    }

    public ByteBuffer ledgerGetEntry(BKExtentId extentId, int fragmentId, int size)
            throws BKException, InterruptedException {
        long entryId = fragmentId;
        byte[] data;

        LedgerHandle lh = elm.getAnyLedgerHandle(extentId);
        if (lh == null) {
            // Let us open the ledger for read in non-recovery mode.
            // Since we use LRU eviction policy for read handles, absence
            // of the handle means that the handle could be evicted or
            // that it was never opened. We cannot differentiate between
            // the two cases.

            lh = ledgerNonRecoveryOpenRead(extentId);
        }

        if (fragmentId == 0) {
            // Trailer
            if (!lh.isClosed()) {
                // Trying to read the trailer of an non closed ledger.
                LOG.info("Trying to read the trailer of extentId {} before closing", extentId);
                throw BKException.create(Code.NoSuchEntryException);
            }
            // This is a closed entry. We need to get last entry
            entryId = lh.getLastAddConfirmed();
        } else { // It is not a trailer
            entryId = fragmentId - 1;
        }

        // Sanity check before trying to read; Get the latest LAC.
        if (entryId > lh.getLastAddConfirmed()) {
            // Get the latest Lac.
            try {
                lh.readLac();
            } catch (BKException e) {
                // We don't need to fail the read just because getting explicit LAC failed.
                LOG.warn("Read lac on extentId: {} failed: {} ", extentId, e);
            }
        }

        if ((entryId > lh.getLastAddConfirmed()) || (entryId == BKPConstants.NO_ENTRY)) {
            if (lh.isClosed()) {
                LOG.info("Trying to read beyond LAC on a closed extentId {}", extentId);
                throw BKException.create(Code.LedgerClosedNoSuchEntryException);
            } else {
                LOG.info("Trying to read beyond LAC on extentId {}", extentId);
                throw BKException.create(Code.NoSuchEntryException);
            }
        }

        Enumeration<LedgerEntry> entries = lh.readEntries(entryId, entryId);
        LedgerEntry e = entries.nextElement();
        data = e.getEntry();
        cByteBuffer.clear();
        cByteBuffer.put(data, 0, Math.min(data.length, cByteBuffer.remaining()));

        return cByteBuffer;
    }

    static class ProxyAddCallback implements AddCallback {
        long entryId = -1;

        /**
         * Implementation of callback interface for asynchronous write method.
         *
         * @param rc
         *          return code
         * @param ledger
         *          ledger identifier
         * @param entry
         *          entry identifier
         * @param ctx
         *          control object
         */
        @Override
        public void addComplete(int rc, LedgerHandle lh, long entry, Object ctx) {
            LedgerAsyncWriteStatus asyncStatus = (LedgerAsyncWriteStatus) ctx;
            asyncStatus.setComplete(rc, entry);
        }
    }
}
