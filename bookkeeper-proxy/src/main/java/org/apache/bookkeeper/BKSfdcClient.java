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
    ByteBuffer cByteBuffer = ByteBuffer.allocate(BKPConstants.MAX_FRAG_SIZE);
    private final static Logger LOG = LoggerFactory.getLogger(BKSfdcClient.class);

    public BKSfdcClient(BookKeeper bk, BKExtentLedgerMap elm) {
        this.bk = bk;
        this.elm = elm;
    }

    public byte ledgerCreate(BKExtentId extentId) {
        try {
            if (elm.extentMapExists(extentId))
                return BKPConstants.SF_ErrorExist;

            lh = bk.createLedgerAdv(extentId.asLong(), 3, 3, 2, DigestType.MAC, "foo".getBytes());
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

    public byte ledgerOpenRead(BKExtentId extentId) {

        LedgerHandle rlh;

        LedgerPrivateData lpd = elm.getLedgerPrivate(extentId);
        
        if (lpd == null){
            // No local mapping, create it
            lpd = elm.createLedgerMap(extentId);
        }

        rlh = lpd.getReadLedgerHandle();
        if (rlh != null) {
            // The ledger is already opened for read.
            // Nothing to do
            return BKPConstants.SF_OK;
        }
        
        // Let us try to open the ledger for read
        try {
            rlh = bk.openLedgerNoRecovery(extentId.asLong(), DigestType.MAC, "foo".getBytes());
        } catch (InterruptedException | BKException e) {
            if ((e instanceof BKException) &&
                    ((BKException)e).getCode() == Code.NoSuchLedgerExistsException) {                
                elm.deleteLedgerPrivate(extentId);
                return BKPConstants.SF_ErrorNotFound;
            }
            LOG.error(e.toString());
            return BKPConstants.SF_InternalError;
        }

        lpd.setReadLedgerHandle(rlh);
        return BKPConstants.SF_OK;
    }

    public boolean ledgerMapExists(BKExtentId extentId) {
        return elm.extentMapExists(extentId);
    }

    public long ledgerStat(BKExtentId extentId) {
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
            ret = ledgerOpenRead(extentId);
            if (ret != BKPConstants.SF_OK) {
                return 0;
            }
            lh = lpd.getReadLedgerHandle();
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

        if (!elm.extentMapExists(extentId)) {
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
            return BKPConstants.SF_InternalError;
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
                if (lpd.getTrailerId() != BKPConstants.NO_ENTRY) {
                    LOG.error("Trying to re-set trailer for the ledger {}", extentId);
                    return BKPConstants.SF_ErrorBadRequest;
                }
                lpd.setTrailerId(entryId);
                // Close the ledger
                ledgerWriteClose(extentId);
            }
        } catch (InterruptedException | BKException e) {
            LOG.error(e.toString());
            return BKPConstants.SF_InternalError;
        }
        return BKPConstants.SF_OK;
    }

    public ByteBuffer ledgerGetEntry(BKExtentId extentId, int fragmentId, int size) {
        int entryId = fragmentId;
        byte[] data;

        try {
            if (!elm.extentMapExists(extentId)) {
                if (ledgerOpenRead(extentId) != BKPConstants.SF_OK)
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
                    ledgerOpenRead(extentId);
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
