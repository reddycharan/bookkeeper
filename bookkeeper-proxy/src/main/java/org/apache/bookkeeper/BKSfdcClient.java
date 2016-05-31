package org.apache.bookkeeper;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Enumeration;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BKException.Code;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.conf.BookKeeperProxyConfiguration;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.LedgerIdFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BKSfdcClient {
    private final static Logger LOG = LoggerFactory.getLogger(BKSfdcClient.class);
    private final BookKeeperProxyConfiguration bkpConfig;
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
		StatsLogger statsLogger) {
        this.bkpConfig = bkpConfig;
        this.bk = bk;
        this.elm = elm;
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

    public void ledgerCreate(BKExtentId extentId) throws BKException, InterruptedException {

        LedgerPrivateData lpd = elm.getLedgerPrivate(extentId);
        if ((lpd != null) && (lpd.getAnyLedgerHandle() != null)) {
            throw BKException.create(Code.LedgerExistException);
        }

        LedgerHandle lh = bk.createLedgerAdv(extentId.asLong(), ensembleSize, writeQuorumSize, ackQuorumSize,
                digestType, password.getBytes());
        if (lpd == null) {
            lpd = elm.createLedgerMap(extentId);
        }
        lpd.setWriteLedgerHandle(lh);
    }

    /*
     * Opens an inactive ledger. i.e no more writes are going in. If this mode is attempted on an active ledger, it will
     * stop accepting any more writes after this operation.
     */

    public LedgerHandle ledgerRecoveryOpenRead(BKExtentId extentId) throws BKException, InterruptedException {

        LedgerHandle lh = null;
        LedgerPrivateData lpd = elm.getLedgerPrivate(extentId);

        if (lpd == null) {
            // No local mapping, create it
            lpd = elm.createLedgerMap(extentId);
        }

        try {
            lpd.acquireLedgerRecoveryOpenLock();
            lh = lpd.getRecoveryReadLedgerHandle();
            if (lh != null) {
                // The ledger is already opened for read.
                // Nothing to do
                return lh;
            }

            // Opening for recovery.
            // The ledger must have closed or this is a crash recovery.
            // In either case we should not have write ledger handle.
            if (lpd.getWriteLedgerHandle() != null) {
                LOG.info("Opening ExtentId: {} in recovery mode while write handle is active.",
                        extentId);
                throw BKException.create(Code.IllegalOpException);
            }

            if (lpd.getRecoveryReadLedgerHandle() == null) {
                // Let us try to open the ledger for read
                lh = bk.openLedger(extentId.asLong(), digestType, password.getBytes());
                lpd.setRecoveryReadLedgerHandle(lh);
                return lh;
            }
        } finally {
            if (lpd != null) {
                lpd.releaseLedgerRecoveryOpenLock();
            }
        }

        return null;
    }

    /*
     * Opens ledger while it is still actively adding(writing) entries.
     * In this mode, ledger is changing hence one may not get the latest and
     * greatest state and entries of the ledger.
     */
    public LedgerHandle ledgerNonRecoveryOpenRead(BKExtentId extentId) throws BKException, InterruptedException {

        LedgerHandle rlh;

        LedgerPrivateData lpd = elm.getLedgerPrivate(extentId);
        if (lpd == null) {
            // No local mapping, create it
            lpd = elm.createLedgerMap(extentId);
        } else {
            rlh = lpd.getNonRecoveryReadLedgerHandle();
            if (rlh != null) {
                // The ledger is already opened for read.
                // Nothing to do
                return rlh;
            }
        }

        // Let us try to open the ledger for read
        rlh = bk.openLedgerNoRecovery(extentId.asLong(), digestType, password.getBytes());
        lpd.setNonRecoveryReadLedgerHandle(rlh);

        return rlh;
    }

    public boolean ledgerMapExists(BKExtentId extentId) {
        return elm.extentMapExists(extentId);
    }

    public long ledgerStat(BKExtentId extentId) throws BKException, InterruptedException {
        LedgerHandle lh = null;
        long lSize = 0;

        LedgerPrivateData lpd = elm.getLedgerPrivate(extentId);
        if (lpd != null) {
            // Check if we have write ledger handle open.
            lh = lpd.getAnyLedgerHandle();
            if (lh != null) {
                lSize = lh.getLength();
                return lSize;
            }
        }

        // No ledger cached locally.
        // Just open/get size and close the extent.
        lh = bk.openLedgerNoRecovery(extentId.asLong(), digestType, password.getBytes());
        if (lh != null) {
            lSize = lh.getLength();
            lh.close();

            return lSize;
        }

        LOG.error("Ledger for extentId: {} does not exist.", extentId);
        return -1;
    }

    public void ledgerDeleteAll() throws BKException, InterruptedException {
        BKExtentId[] ledgerIdList = elm.getAllExtentIds();
        for (int i = 0; i < ledgerIdList.length; i++) {
            this.ledgerDelete(ledgerIdList[i]);
        }
    }

    public Iterable<Long> ledgerList() throws IOException {
        BookKeeperAdmin admin = new BookKeeperAdmin(bk);
        return admin.listLedgers();
    }

    public void ledgerWriteClose(BKExtentId extentId) throws BKException, InterruptedException {

        LedgerPrivateData lpd = elm.getLedgerPrivate(extentId);
        if (lpd == null) {
            // No Extent just return OK
            return;
        }

        LedgerHandle wlh = lpd.getWriteLedgerHandle();
        if (wlh != null) {
            wlh.close();
            lpd.setWriteLedgerHandle(null);
        }
    }

    public void ledgerReadClose(BKExtentId extentId) throws BKException, InterruptedException {

        LedgerPrivateData lpd = elm.getLedgerPrivate(extentId);
        if (lpd == null) {
            // No Extent just return OK
            return;
        }

        LedgerHandle rrlh = lpd.getRecoveryReadLedgerHandle();
        LedgerHandle rlh = lpd.getNonRecoveryReadLedgerHandle();
        if (rrlh != null) {
            rrlh.close();
            lpd.setRecoveryReadLedgerHandle(null);
        }
        if (rlh != null) {
            rlh.close();
            lpd.setNonRecoveryReadLedgerHandle(null);
        }
    }

    public void ledgerDelete(BKExtentId ledgerId) throws BKException, InterruptedException {

        bk.deleteLedger(ledgerId.asLong());

        LedgerPrivateData lpd = elm.getLedgerPrivate(ledgerId);
        if (lpd != null) {
            elm.deleteLedgerPrivate(ledgerId);
        }
    }

    public void ledgerPutEntry(BKExtentId extentId, int fragmentId, ByteBuffer bdata) throws BKException, InterruptedException {

        long entryId;

        LedgerPrivateData lpd = elm.getLedgerPrivate(extentId);
        if (lpd == null) {
            LOG.error("Attempt to write to extentId : {} with no ledger private data.", extentId);
            throw BKException.create(Code.IllegalOpException);
        }

        LedgerHandle lh = lpd.getWriteLedgerHandle();
        if (lh == null) {
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
        // 1. FragmentId = entryId + 1; first entryId is 0 and first FragmentId
        // = 1
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
            tmpEntryId = lh.getLastAddConfirmed() + 1;
        }
        // Try to catch out of sequence addEntry.
        // Condition below is to check and reduce number of log messages for sequential addEntrys through this worker thread.
        // If multiple threads are adding entries simultaneously, we may log the debug message anyway.
        // This is temporary debug message to catch out of order writes.
        long tmpLac = lh.getLastAddConfirmed();
        if ((tmpLac + 1) != tmpEntryId) {
            LOG.info("Sending non-sequential addEntry. LedgerId:{} currentEntryId:{}, LAC:{}",
                    new Object[] {extentId, tmpEntryId, tmpLac});
        }
        entryId = lh.addEntry(tmpEntryId, bdata.array(), 0, bdata.limit());
        assert (entryId == tmpEntryId);

        // Handle Trailer
        if (fragmentId == 0) {
            // Close the ledger
            ledgerWriteClose(extentId);
        }
    }

    public ByteBuffer ledgerGetEntry(BKExtentId extentId, int fragmentId, int size) throws BKException, InterruptedException {
        long entryId = fragmentId;
        byte[] data;

        LedgerHandle lh = null;
        LedgerPrivateData lpd = elm.getLedgerPrivate(extentId);
        if (lpd != null) {
            // If we are the writer, we will have write ledger handle
            // and it will have the most recent information.
            lh = lpd.getAnyLedgerHandle();
        }

        if (lh == null) {
            // Let us open the ledger for read in recovery mode.
            // All reads without prior opens will be performed
            // through opening the ledger in recovery mode.

            lh = ledgerRecoveryOpenRead(extentId);
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
            lh.readLac();
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
}
