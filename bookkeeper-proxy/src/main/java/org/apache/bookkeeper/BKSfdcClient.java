package org.apache.bookkeeper;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Enumeration;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BKException.Code;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.LedgerHandleAdv;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BKSfdcClient {
    BKExtentLedgerMap elm = null;
    long ledgerId = LedgerHandle.INVALID_ENTRY_ID;
    BookKeeper bk = null;
    LedgerHandle lh = null;
    LedgerEntry ledgerEntry = null;
    Object ledgerObj = null;
    boolean exists = false;
    ByteBuffer cByteBuffer = ByteBuffer.allocate(BKPConstants.MAX_FRAG_SIZE);
    private final static Logger LOG = LoggerFactory.getLogger(BKSfdcClient.class);

    public BKSfdcClient(BookKeeper bk, BKExtentLedgerMap elm) {
        this.bk = bk;
        this.elm = elm;
    }

    public byte LedgerCreate(String extentId) {
        try {
            if (elm.extentExists(extentId))
                return BKPConstants.SF_ErrorExist;

            lh = bk.createLedgerAdv(3, 3, 2, DigestType.MAC, "foo".getBytes());
            elm.createLedgerMap(extentId, lh);

        } catch (InterruptedException | BKException e) {
            LOG.error(e.toString());
            // TODO: Should return an error status came from BK;
            // TODO: map BK errors into SFStore errors
            return BKPConstants.SF_InternalError;
        }

        return BKPConstants.SF_OK;
    }

    public byte LedgerOpenRead(String extentId) {

        LedgerHandle rlh;

        if (!elm.extentExists(extentId))
            return BKPConstants.SF_ErrorNotFound;

        LedgerPrivateData lpd = elm.getLedgerPrivate(extentId);

        rlh = lpd.getReadLedgerHandle();

        if (rlh != null) {
            // The ledger is already opened for read.
            // Nothing to do
            return BKPConstants.SF_OK;
        }

        // Let us check if we have LedgerID
        long lId = lpd.getLedgerId();

        try {
            rlh = bk.openLedgerNoRecovery(lId, DigestType.MAC, "foo".getBytes());
        } catch (BKException | InterruptedException e) {
            LOG.error(e.toString());
            e.printStackTrace();
            return BKPConstants.SF_InternalError;
        }
        elm.getLedgerPrivate(extentId).setReadLedgerHandle(rlh);
        return BKPConstants.SF_OK;
    }

    public long LedgerNextEntry(String extentId) {
        return (elm.getLedgerPrivate(extentId).allocateEntryId());
    }

    public boolean LedgerExists(String extentId) {
        return elm.extentExists(extentId);
    }

    public long LedgerStat(String extentId) {
        long ledgerSize;
        LedgerPrivateData lpd = elm.getLedgerPrivate(extentId);

        // Check if we have write ledger handle open.
        LedgerHandle lh = lpd.getWriteLedgerHandle();

        if (lh == null) {
            // Check if we have read leader handle
            lh = lpd.getReadLedgerHandle();
        }

        if (lh == null) {
            byte ret;
            // Don't have ledger opened, open it for read.
            ret = LedgerOpenRead(extentId);
            if (ret != BKPConstants.SF_OK) {
                return 0;
            }
            lh = lpd.getReadLedgerHandle();
        }

        // We have a ledger handle.
        ledgerSize = lh.getLength();
        return ledgerSize;
    }

    public void LedgerDeleteAll() {
        String[] ledgerIdList = elm.getAllExtentIds();
        for (int i = 0; i < ledgerIdList.length; i++) {
            this.LedgerDelete(ledgerIdList[i]);
        }
        elm.deleteAllLedgerHandles();
    }

    public String[] LedgerList() {
        return elm.getAllExtentIds();
    }

    public byte LedgerWriteClose(String extentId) {

        if (!elm.extentExists(extentId)) {
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
            return BKPConstants.SF_ErrorNotFound;
        }
        return BKPConstants.SF_OK;
    }

    public byte LedgerReadClose(String extentId) {

        if (!elm.extentExists(extentId)) {
            // No Extent just return OK
            return BKPConstants.SF_OK;
        }

        lh = elm.getLedgerPrivate(extentId).getReadLedgerHandle();

        if (lh == null)
            return BKPConstants.SF_OK;

        try {
            lh.close();
            // Reset the Ledger Handle
            elm.getLedgerPrivate(extentId).setReadLedgerHandle(null);
        } catch (InterruptedException | BKException e) {
            LOG.error(e.toString());
            e.printStackTrace();
            return BKPConstants.SF_ErrorNotFound;
        }
        return BKPConstants.SF_OK;
    }

    public byte LedgerDelete(String extentId) {

        try {
            if (!elm.extentExists(extentId))
                return BKPConstants.SF_ErrorNotFound;
            long lId = elm.getLedgerPrivate(extentId).getLedgerId();
            bk.deleteLedger(lId);
            elm.deleteLedgerPrivate(extentId);
        } catch (InterruptedException | BKException e) {
            LOG.error(e.toString());
            return BKPConstants.SF_InternalError;
        }

        return BKPConstants.SF_OK;
    }

    public byte LedgerPutEntry(String extentId, int fragmentId, ByteBuffer bdata) {
        long entryId;

        try {
            exists = elm.extentExists(extentId);
            if (exists == false) {
                return BKPConstants.SF_InternalError;
            }

            LedgerPrivateData lpd = elm.getLedgerPrivate(extentId);

            lh = lpd.getWriteLedgerHandle();

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
            if (fragmentId != 0)
                tmpEntryId = fragmentId - 1;
            else
                tmpEntryId = lpd.getLastWriteEntryId() + 1;
            entryId = lh.addEntry(tmpEntryId, bdata.array(), 0, bdata.limit());
            assert(entryId == tmpEntryId);
            lpd.setLastWriteEntryId(entryId);

            // Handle Trailer
            if (fragmentId == 0) {
                if (lpd.getTrailerId() != BKPConstants.NO_ENTRY) {
                    LOG.error("Trying to re-set trailer for the ledger {}", extentId);
                    return BKPConstants.SF_ErrorBadRequest;
                }
                lpd.setTrailerId(entryId);
                // Close the ledger
                LedgerWriteClose(extentId);
            }
        } catch (InterruptedException | BKException e) {
            LOG.error(e.toString());
            return BKPConstants.SF_InternalError;
        }
        return BKPConstants.SF_OK;
    }

    public ByteBuffer LedgerGetEntry(String extentId, int fragmentId, int size) {
        int entryId = fragmentId;
        byte[] data;

        try {
            exists = elm.extentExists(extentId);
            if (exists == false) {
                return null;
            }

            LedgerPrivateData lpd = elm.getLedgerPrivate(extentId);
            // If we are the writer, we will have write ledger handle
            // and it will have the most recent information.
            // So let us try to get write ledger handle first.
            lh = lpd.getWriteLedgerHandle();
            if (lh == null) {
                lh = lpd.getReadLedgerHandle();
                if (lh == null) {
                    // Let us open the ledger for read
                    LedgerOpenRead(extentId);
                    lh = lpd.getReadLedgerHandle();
                }
            }

            Enumeration<LedgerEntry> entries = null;

            if (fragmentId == 0) {
                // Trailer
                entryId = (int) lpd.getTrailerId();
                if (entryId == BKPConstants.NO_ENTRY) {
                    if (!lh.isClosed()) {
                        // Trying to read the trailer of an non closed ledger.
                        // TODO: Throw an exception?
                        LOG.info("Trying to read the trailer of Extent: {} before closing", extentId);
                        return null;
                    }
                    // This is a closed entry. We need to get last entry through
                    // protocol
                    // and update our local cache.
                    lpd.setTrailerId(lh.getLastAddConfirmed());
                    entryId = (int) lpd.getTrailerId();
                }
            } else { // It is not a trailer
                entryId = fragmentId - 1;
                if (entryId == lpd.getTrailerId()) {
                    // user can't refer trailer with fragmentId. return NULL
                    return null;
                }
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
        } catch (BKException bke) {
            if (bke.getCode() != Code.ReadException) {
                // SDB tries to find the end of the Extent by reading until it gets an error.
                // Current readEntries() returns BKReadException in this case.
                // Since it is valid error for us, skip printing error for this error.
                LOG.error(bke.toString());
                bke.printStackTrace();
            }
            return null;
        }
        return cByteBuffer;
    }
}
