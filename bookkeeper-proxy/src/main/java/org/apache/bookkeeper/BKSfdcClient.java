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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BKSfdcClient {
    private final BookKeeperProxyConfiguration bkpConfig;
    BKExtentLedgerMap elm = null;
    long ledgerId = LedgerHandle.INVALID_ENTRY_ID;
    BookKeeper bk = null;
    // LedgerHandle lh = null;
    LedgerEntry ledgerEntry = null;
    Object ledgerObj = null;
    ByteBuffer cByteBuffer;
    private final static Logger LOG = LoggerFactory.getLogger(BKSfdcClient.class);
    private int ensembleSize;
    private int writeQuorumSize;
    private int ackQuorumSize;
    private String password;
    private DigestType digestType;

    public BKSfdcClient(BookKeeperProxyConfiguration bkpConfig, BookKeeper bk, BKExtentLedgerMap elm) {
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

    public byte ledgerCreate(BKExtentId extentId) {
        try {
            if (elm.extentMapExists(extentId))
                return BKPConstants.SF_ErrorExist;

            LedgerHandle lh = bk.createLedgerAdv(extentId.asLong(), ensembleSize, writeQuorumSize, ackQuorumSize, digestType,
                    password.getBytes());
            LedgerPrivateData lpd = elm.createLedgerMap(extentId);
            try {
                lpd.setWriteLedgerHandle(lh);
            } finally {
                try {
                    lpd.putWriteLedgerHandle();
                } catch (Exception e) {
                    LOG.error("Exception while relinquishing Write Ledger Handle for extentId:{}",
                            extentId.asHexString(), e);
                }
            }
        } catch (BKException e) {
            LOG.error("Exception in creating extentId {}: ", extentId.asHexString(), e);
            if (e.getCode() == Code.LedgerExistException) {
                return BKPConstants.SF_ErrorExist;
            }
            return BKPConstants.SF_InternalError;
        } catch (InterruptedException e) {
            LOG.error("Operation interrupted when creating extentId {}: ", extentId.asHexString(), e);
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
            try {
                lh = lpd.getRecoveryReadLedgerHandle();
                if (lh != null) {
                    // The ledger is already opened for read.
                    // Nothing to do
                    return BKPConstants.SF_OK;
                }
            } finally {
                try {
                    lpd.putRecoveryReadLedgerHandle();
                } catch (Exception e) {
                    LOG.error("Exception while relinquishing Recovery Read Ledger Handle for extentId:{}",
                            extentId.asHexString(), e);
                }
            }

            // Opening for recovery.
            // The ledger must have closed or this is a crash recovery.
            // In either case we should not have write ledger handle.
            try {
                if (lpd.getWriteLedgerHandle() != null) {
                    LOG.info("Opening ExtentId: {} in recovery mode while write open is active.",
                            extentId.asHexString());
                    return BKPConstants.SF_ErrorBadRequest;
                }
            } finally {
                try {
                    lpd.putWriteLedgerHandle();
                } catch (Exception e) {
                    LOG.error("Exception while relinquishing Write Ledger Handle for extentId:{}",
                            extentId.asHexString(), e);
                }
            }
        }

        lpd.acquireLedgerRecoveryOpenLock();
        //AMK: check if already recovered here since it may have been getting recovered when we were waiting for a lock
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
                    LOG.error("Unable to open ledger for ententId: {} :", extentId.asHexString(), e);
                    return BKPConstants.SF_InternalError;
                }

                try {
                    lpd.setRecoveryReadLedgerHandle(lh);
                } finally {
                    try {
                        lpd.putRecoveryReadLedgerHandle();
                    } catch (Exception e) {
                        LOG.error("Exception while relinquishing Recovery Read Ledger Handle for extentId:{}", extentId.asHexString(), e);
                    }
                }
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
            try {
                rlh = lpd.getNonRecoveryReadLedgerHandle();
                if (rlh != null) {
                    // The ledger is already opened for read.
                    // Nothing to do
                    return BKPConstants.SF_OK;
                }
            } finally {
                try {
                    lpd.putNonRecoveryReadLedgerHandle();
                } catch (Exception e) {
                    LOG.error("Exception while relinquishing Non-recovery Read Ledger Handle for extentId:{}",
                            extentId.asHexString(), e);
                }
            }
        }

        // Let us try to open the ledger for read
        try {
            rlh = bk.openLedgerNoRecovery(extentId.asLong(), digestType, password.getBytes());
        } catch (BKException e) {
            if (e.getCode() == Code.NoSuchLedgerExistsException) {
                elm.deleteLedgerPrivate(extentId);
                return BKPConstants.SF_ErrorNotFound;
            }
            LOG.error("Exception in opening ledger for recovery for extentID {}: ", extentId.asHexString(), e);
            return BKPConstants.SF_InternalError;
        } catch (InterruptedException ie) {
            LOG.error("Exception in opening ledger for recovery for extentID {}: ", extentId.asHexString(), ie);
            return BKPConstants.SF_InternalError;
        }

        try {
            lpd.setNonRecoveryReadLedgerHandle(rlh);
        } finally {
            try {
                lpd.putNonRecoveryReadLedgerHandle();
            } catch (Exception e) {
                LOG.error("Exception while relinquishing Non-recovery Read Ledger Handle for extentId:{}",
                        extentId.asHexString(), e);
            }
        }
        return BKPConstants.SF_OK;
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
            try {
                lh = lpd.getAnyLedgerHandle();
                if (lh != null) {
                    lSize = lh.getLength();
                    return lSize;
                }
            } finally {
                lpd.putSpecifiedLedgerHandle(lh);
            }
        }

        // No ledger cached locally.
        // Just open/get size and close the extent.
        lh = bk.openLedgerNoRecovery(extentId.asLong(), digestType, password.getBytes());
        lSize = lh.getLength();
        lh.close();

        return lSize;
    }

    public void ledgerDeleteAll() {
        BKExtentId[] ledgerIdList = elm.getAllExtentIds();
        for (int i = 0; i < ledgerIdList.length; i++) {
            this.ledgerDelete(ledgerIdList[i]);
        }
    }

    public Iterable<Long> ledgerList() throws IOException {
        BookKeeperAdmin admin = new BookKeeperAdmin(bk);
        return admin.listLedgers();
    }

    private byte ledgerWriteClose(BKExtentId extentId, boolean force) {

        LedgerPrivateData lpd = elm.getLedgerPrivate(extentId);
        if (lpd == null) {
            // No Extent just return OK
            return BKPConstants.SF_OK;
        }

        try {
            lpd.closeWriteLedgerHandle(force);
        } catch (Exception e) {
            LOG.error("Exception while closing bookkeeper write handle for extentId {}: ", extentId.asHexString(), e);
            return BKPConstants.SF_InternalError;
        }

        return BKPConstants.SF_OK;
    }

    private byte ledgerReadClose(BKExtentId extentId, boolean force) {

        LedgerPrivateData lpd = elm.getLedgerPrivate(extentId);
        if (lpd == null) {
            // No Extent just return OK
            return BKPConstants.SF_OK;
        }

        try {
            lpd.closeRecoveryReadLedgerHandle(force);
            lpd.closeNonRecoveryReadLedgerHandle(force);
        } catch (Exception e) {
            LOG.error("Exception while closing bookkeeper read handles for extentId {}: ", extentId.asHexString(), e);
            return BKPConstants.SF_InternalError;
        }
        return BKPConstants.SF_OK;
    }

    public byte ledgerWriteUnsafeClose(BKExtentId extentId) {
        return ledgerWriteClose(extentId, true);
    }

    public byte ledgerReadUnsafeClose(BKExtentId extentId) {
        return ledgerReadClose(extentId, true);
    }

    public byte ledgerWriteSafeClose(BKExtentId extentId) {
        return ledgerWriteClose(extentId, false);
    }

    public byte ledgerReadSafeClose(BKExtentId extentId) {
        return ledgerReadClose(extentId, false);
    }

    public byte ledgerDelete(BKExtentId ledgerId) {

        LedgerPrivateData lpd = elm.getLedgerPrivate(ledgerId);
        if (lpd != null) {
            elm.deleteLedgerPrivate(ledgerId);

            if (!lpd.isLedgerClosed()) {
                LOG.warn("ExtentId needs to be closed before deletion: " + ledgerId.asHexString());
                /* throw new IllegalStateException("The extentId" +ledgerId.asHexString()
                    + "needs to be closed before deletion"); */
            }
        }

        try {
            bk.deleteLedger(ledgerId.asLong());
        } catch (BKException e) {
            if (e.getCode() == Code.NoSuchLedgerExistsException) {
                LOG.warn("Unable to find extentId {}: ", ledgerId.asHexString(), e);
                return BKPConstants.SF_ErrorNotFound;
            }
            LOG.error("Error while deleting extentId {}: ", ledgerId.asHexString(), e);
            return BKPConstants.SF_InternalError;
        } catch (InterruptedException e) {
            LOG.error("Error while deleting extentId {}:", ledgerId.asHexString(), e);
            return BKPConstants.SF_InternalError;
        }

        return BKPConstants.SF_OK;
    }

    public byte ledgerPutEntry(BKExtentId extentId, int fragmentId, ByteBuffer bdata) {
        long entryId;

        try {
            LedgerPrivateData lpd = elm.getLedgerPrivate(extentId);
            if (lpd == null) {
                return BKPConstants.SF_ErrorNotFound;
            }

            try {
                LedgerHandle lh = lpd.getWriteLedgerHandle();
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
                    tmpEntryId = lh.getLastAddConfirmed() + 1;
                }
                entryId = lh.addEntry(tmpEntryId, bdata.array(), 0, bdata.limit());
                assert(entryId == tmpEntryId);

            } finally {
                lpd.putWriteLedgerHandle();
            }

            // Handle Trailer
            if (fragmentId == 0) {
                // Close the ledger
                ledgerWriteSafeClose(extentId);
            }

        } catch (Exception e) {
            LOG.error("Exception in putting an entry into extentId {}: ", extentId.asHexString(), e);
            return BKPConstants.SF_InternalError;
        }
        return BKPConstants.SF_OK;
    }

    public ByteBuffer ledgerGetEntry(BKExtentId extentId, int fragmentId, int size) throws BKException {
        long entryId = fragmentId;
        byte[] data;

        try {
            if (extentId == null) { 
                // We need the above check as extentMapExists below will throw NPE
                // if extentId is null.
                return null;
            }

            LedgerPrivateData lpd = elm.getLedgerPrivate(extentId);
            if (lpd == null) {
                if (ledgerRecoveryOpenRead(extentId) != BKPConstants.SF_OK) {
                    return null;
                }
                lpd = elm.getLedgerPrivate(extentId);
            }

            LedgerHandle lh = null;
            try {
                // If we are the writer, we will have write ledger handle
                // and it will have the most recent information.
                lh = lpd.getAnyLedgerHandle();
                if (lh == null) {
                    // Let us open the ledger for read in recovery mode.
                    // All reads without prior opens will be performed
                    // through opening the ledger in recovery mode.
                    if (ledgerRecoveryOpenRead(extentId)  != BKPConstants.SF_OK) {
                        return null;
                    }
                    lh = lpd.getRecoveryReadLedgerHandle(); // AMK: is it a good idea to read from recovery handle ?
                }

                if (fragmentId == 0) {
                    // Trailer
                    if (!lh.isClosed()) {
                        // Trying to read the trailer of an non closed ledger.
                        LOG.info("Trying to read the trailer of extentId {} before closing", extentId.asHexString());
                        return null;
                    }
                    // This is a closed entry. We need to get last entry
                    entryId = lh.getLastAddConfirmed();
                } else { // It is not a trailer
                    entryId = fragmentId - 1;
                }

                // Sanity check before trying to read
                if (lh.isClosed() && entryId > lh.getLastAddConfirmed()) {
                    LOG.info("Trying to read beyond LAC on a closed extentId {}", extentId.asHexString());
                    throw BKException.create(Code.LedgerClosedException);
                }

                Enumeration<LedgerEntry> entries = lh.readEntries(entryId, entryId);
                LedgerEntry e = entries.nextElement();
                data = e.getEntry();
                cByteBuffer.clear();
                cByteBuffer.put(data, 0, Math.min(data.length, cByteBuffer.remaining()));
            } finally {
                lpd.putSpecifiedLedgerHandle(lh);
            }
        } catch (InterruptedException e) {
            LOG.error("Exception while getting an element from extentId {}", extentId.asHexString(), e);
            return null;
        }
        return cByteBuffer;
    }
}
