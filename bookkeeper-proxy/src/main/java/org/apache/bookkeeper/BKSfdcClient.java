package org.apache.bookkeeper;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Enumeration;
import java.util.concurrent.locks.Lock;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BKException.Code;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.conf.BookKeeperProxyConfiguraiton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BKSfdcClient {
    private final BookKeeperProxyConfiguraiton bkpConfig;
    BKExtentLedgerMap elm = null;
    long ledgerId = LedgerHandle.INVALID_ENTRY_ID;
    BookKeeper bk = null;
    LedgerHandle lh = null;
    LedgerEntry ledgerEntry = null;
    Object ledgerObj = null;
    ByteBuffer cByteBuffer;
    private final static Logger LOG = LoggerFactory.getLogger(BKSfdcClient.class);
    private int ensembleSize;
    private int writeQuorumSize;
    private int ackQuorumSize;
    private String password;
    private DigestType digestType;

    public BKSfdcClient(BookKeeperProxyConfiguraiton bkpConfig, BookKeeper bk, BKExtentLedgerMap elm) {
        this.bkpConfig = bkpConfig;
        this.bk = bk;
        this.elm = elm;
        cByteBuffer = ByteBuffer.allocate(bkpConfig.getMaxFragSize());
        this.ensembleSize = bkpConfig.getEnsembleSize();
        this.writeQuorumSize = bkpConfig.getWriteQuorumSize();
        this.ackQuorumSize = bkpConfig.getAckQuorumSize();
        this.password = bkpConfig.getPassword();
        this.digestType = bkpConfig.getDigestType();
    }

    public byte ledgerCreate(BKExtentId extentId) {
        try {
            if (elm.extentMapExists(extentId))
                return BKPConstants.SF_ErrorExist;

            lh = bk.createLedgerAdv(extentId.asLong(), ensembleSize, writeQuorumSize, ackQuorumSize, digestType,
                    password.getBytes());
            elm.createLedgerMap(extentId).setWriteLedgerHandle(lh);
        } catch (BKException e) {
            LOG.error(e.toString());
            if (e.getCode() == Code.LedgerExistException) {
                return BKPConstants.SF_ErrorExist;
            }
            return BKPConstants.SF_InternalError;
        } catch (InterruptedException e) {
            LOG.error(e.toString());
            return BKPConstants.SF_InternalError;
        }

        return BKPConstants.SF_OK;
    }

    /*
     * Opens an inactive ledger. i.e no more writes are going in. If this mode is attempted on an active ledger, it will
     * stop accepting any more writes after this operation.
     */
    public byte ledgerRecoveryOpenRead(BKExtentId extentId) {

        LedgerHandle lh;

        LedgerPrivateData lpd = elm.getLedgerPrivate(extentId);

        if (lpd == null) {
            // No local mapping, create it
            lpd = elm.createLedgerMap(extentId);
        } else {
            lh = lpd.getRecoveryReadLedgerHandle();
            if (lh != null) {
                // The ledger is already opened for read.
                // Nothing to do
                return BKPConstants.SF_OK;
            }

            // Opening for recovery.
            // The ledger must have closed or this is a crash recovery.
            // In either case we should not have write ledger handle.
            if (lpd.getWriteLedgerHandle() != null) {
                LOG.info("Opening ExtentId: {} in recovery mode while write open is active.",
                        extentId.asHexString());
                return BKPConstants.SF_ErrorBadRequest;
            }
        }
        
        lpd.acquireLedgerRecoveryOpenLock();
        try {
            if (lpd.getRecoveryReadLedgerHandle() == null) {
                // Let us try to open the ledger for read
                try {
                    lh = bk.openLedger(extentId.asLong(), digestType, password.getBytes());
                } catch (InterruptedException | BKException e) {
                    if ((e instanceof BKException) && ((BKException) e).getCode() == Code.NoSuchLedgerExistsException) {
                        elm.deleteLedgerPrivate(extentId);
                        return BKPConstants.SF_ErrorNotFound;
                    }
                    LOG.error(e.toString());
                    return BKPConstants.SF_InternalError;
                }

                lpd.setRecoveryReadLedgerHandle(lh);
            }
        } finally {
            lpd.releaseLedgerRecoveryOpenLock();
        }
        return BKPConstants.SF_OK;
    }

    /*
     * Opens ledger while it is still actively adding(writing) entries.
     * In this mode, ledger is changing hence one may not get the latest and
     * greatest state and entries of the ledger.
     */
    public byte ledgerNonRecoveryOpenRead(BKExtentId extentId) {

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
                return BKPConstants.SF_OK;
            }
        }

        // Let us try to open the ledger for read
        try {
            rlh = bk.openLedgerNoRecovery(extentId.asLong(), digestType, password.getBytes());
        } catch (InterruptedException | BKException e) {
            if ((e instanceof BKException) && ((BKException) e).getCode() == Code.NoSuchLedgerExistsException) {
                elm.deleteLedgerPrivate(extentId);
                return BKPConstants.SF_ErrorNotFound;
            }
            LOG.error(e.toString());
            return BKPConstants.SF_InternalError;
        }

        lpd.setNonRecoveryReadLedgerHandle(rlh);
        return BKPConstants.SF_OK;
    }

    public boolean ledgerMapExists(BKExtentId extentId) {
        return elm.extentMapExists(extentId);
    }

    public long ledgerStat(BKExtentId extentId) {
        long ledgerSize;
        LedgerPrivateData lpd = elm.getLedgerPrivate(extentId);

        // Check if we have write ledger handle open.
        LedgerHandle lh = lpd.getAnyLedgerHandle();

        if (lh == null) {
            byte ret;
            // Don't have ledger opened, open it for read.
            ret = ledgerNonRecoveryOpenRead(extentId);
            if (ret != BKPConstants.SF_OK) {
                return 0;
            }
            lh = lpd.getAnyLedgerHandle();
        }

        // We have a ledger handle.
        ledgerSize = lh.getLength();
        return ledgerSize;
    }

    public void ledgerDeleteAll() {
        BKExtentId[] ledgerIdList = elm.getAllExtentIds();
        for (int i = 0; i < ledgerIdList.length; i++) {
            this.ledgerDelete(ledgerIdList[i]);
        }
        elm.deleteAllLedgerHandles();
    }

    public Iterable<Long> ledgerList() throws IOException {
        BookKeeperAdmin admin = new BookKeeperAdmin(bk);
        return admin.listLedgers();
    }

    public byte ledgerWriteClose(BKExtentId extentId) {

        if (!elm.extentMapExists(extentId)) {
            // No Extent just return OK
            return BKPConstants.SF_OK;
        }

        lh = elm.getLedgerPrivate(extentId).getWriteLedgerHandle();

        if (lh == null)
            return BKPConstants.SF_OK;

        try {
            lh.close();
            // Reset Ledger Handle
            elm.getLedgerPrivate(extentId).setWriteLedgerHandle(null);
        } catch (InterruptedException | BKException e) {
            LOG.error(e.toString());
            e.printStackTrace();
            return BKPConstants.SF_InternalError;
        }
        return BKPConstants.SF_OK;
    }

    public byte ledgerReadClose(BKExtentId extentId) {

        LedgerHandle nrrlh, rrlh;
        if (!elm.extentMapExists(extentId)) {
            // No Extent just return OK
            return BKPConstants.SF_OK;
        }
        // Close all read ledger handles.

        nrrlh = elm.getLedgerPrivate(extentId).getNonRecoveryReadLedgerHandle();
        rrlh = elm.getLedgerPrivate(extentId).getRecoveryReadLedgerHandle();

        // If we have no ledger handle opened for read, return success.
        if ((nrrlh == null) && (rrlh == null))
            return BKPConstants.SF_OK;

        // Take care of non-recovery ledger handle.
        if (nrrlh != null) {
            try {
                nrrlh.close();
                // Reset the Ledger Handle
                elm.getLedgerPrivate(extentId).setNonRecoveryReadLedgerHandle(null);
            } catch (InterruptedException | BKException e) {
                LOG.error(e.toString());
                e.printStackTrace();
                return BKPConstants.SF_InternalError;
            }
        }

        // Take care of recovery ledger handle.
        if (rrlh != null)
        {
            try {
                rrlh.close();
                // Reset the Ledger Handle
                elm.getLedgerPrivate(extentId).setRecoveryReadLedgerHandle(null);
            } catch (InterruptedException | BKException e) {
                LOG.error(e.toString());
                e.printStackTrace();
                return BKPConstants.SF_InternalError;
            }
        }
        return BKPConstants.SF_OK;
    }

    public byte ledgerDelete(BKExtentId ledgerIdList) {
        try {
            bk.deleteLedger(ledgerIdList.asLong());
            elm.deleteLedgerPrivate(ledgerIdList);
        } catch (BKException e) {
            if (e.getCode() == Code.NoSuchLedgerExistsException) {
                return BKPConstants.SF_ErrorNotFound;
            }
            LOG.error(e.toString());
            return BKPConstants.SF_InternalError;
        } catch (InterruptedException e) {
            LOG.error(e.toString());
            return BKPConstants.SF_InternalError;
        }

        return BKPConstants.SF_OK;
    }

    public byte ledgerPutEntry(BKExtentId extentId, int fragmentId, ByteBuffer bdata) {
        long entryId;

        try {
            if (!elm.extentMapExists(extentId)) {
                return BKPConstants.SF_ErrorNotFound;
            }

            LedgerPrivateData lpd = elm.getLedgerPrivate(extentId);

            lh = lpd.getWriteLedgerHandle();
            if (lh == null) {
                return BKPConstants.SF_InternalError;
            }

            // TODO: verify checksum

            // API Requirement:
            // Need to map first and last fragmentIds between SDB and BK.
            //
            // 1. SDB treats FragmentId 0 as the last entry.
            // For BK it is really the last entry (highest entryId of the ledger).
            // 2. FragmentId 1 is the first entry for SDB, but
            // entryId 1 is the second entry for BK as it starts entryId with 0.
            //
            // Rules:
            // 1. FragmentId = entryId + 1; first entryId is 0 and first FragmentId = 1
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
                tmpEntryId = lpd.getWriteLedgerHandle().getLastAddConfirmed() + 1;
            }
            entryId = lh.addEntry(tmpEntryId, bdata.array(), 0, bdata.limit());
            assert(entryId == tmpEntryId);

            // Handle Trailer
            if (fragmentId == 0) {
                // Close the ledger
                ledgerWriteClose(extentId);
            }
        } catch (InterruptedException | BKException e) {
            LOG.error(e.toString());
            return BKPConstants.SF_InternalError;
        }
        return BKPConstants.SF_OK;
    }

    public ByteBuffer ledgerGetEntry(BKExtentId extentId, int fragmentId, int size) throws BKException {
        long entryId = fragmentId;
        byte[] data;

        try {
            if (!elm.extentMapExists(extentId)) {
                if (ledgerRecoveryOpenRead(extentId) != BKPConstants.SF_OK)
                    return null;
            }
        
            LedgerPrivateData lpd = elm.getLedgerPrivate(extentId);
            // If we are the writer, we will have write ledger handle
            // and it will have the most recent information.
            lh = lpd.getAnyLedgerHandle();
            if (lh == null) {
                // Let us open the ledger for read in recovery mode.
                // All reads without prior opens will be performed
                // through opening the ledger in recovery mode.
                if (ledgerRecoveryOpenRead(extentId)  != BKPConstants.SF_OK)
                    return null;
                lh = lpd.getRecoveryReadLedgerHandle();
            }

            Enumeration<LedgerEntry> entries = null;

            if (fragmentId == 0) {
                // Trailer
                if (!lh.isClosed()) {
                    // Trying to read the trailer of an non closed ledger.
                    LOG.info("Trying to read the trailer of Extent: {} before closing", extentId.asHexString());
                    return null;
                }
                // This is a closed entry. We need to get last entry
                entryId = lh.getLastAddConfirmed();
            } else { // It is not a trailer
                entryId = fragmentId - 1;
            }

            // Sanity check before trying to read
            if (lh.isClosed() && entryId > lh.getLastAddConfirmed()) {
                LOG.info("Trying to read beyond LAC on a closed ledger: {}", extentId.asHexString());
                throw BKException.create(Code.LedgerClosedException);
            }

            entries = lh.readEntries(entryId, entryId);
            LedgerEntry e = entries.nextElement();

            data = e.getEntry();
            cByteBuffer.clear();
            cByteBuffer.put(data, 0, Math.min(data.length, cByteBuffer.remaining()));
        } catch (InterruptedException ie) {
            LOG.error(ie.toString());
            ie.printStackTrace();
            return null;
        }
        return cByteBuffer;
    }
}
