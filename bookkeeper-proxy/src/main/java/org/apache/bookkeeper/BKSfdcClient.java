package org.apache.bookkeeper;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Enumeration;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.zookeeper.KeeperException;

public class BKSfdcClient {
	BKExtentLedgerMap elm = null;
	long ledgerId = LedgerHandle.INVALID_ENTRY_ID;
	BookKeeper bk = null;
	LedgerHandle lh = null;
	LedgerEntry ledgerEntry = null;
	Object ledgerObj = null;
	boolean exists = false;
	ByteBuffer cByteBuffer = ByteBuffer.allocate(BKPConstants.MAX_FRAG_SIZE);

	public BKSfdcClient(BookKeeper bk, BKExtentLedgerMap elm) {
		this.bk = bk;
		this.elm = elm;
	}

	public byte LedgerCreate(String extentId) {
		try {
			if (elm.extentExists(extentId))
				return BKPConstants.SF_ErrorExist;

			lh = bk.createLedger(3, 2, DigestType.MAC, "foo".getBytes());
			elm.createLedgerMap(extentId, lh);

		} catch (InterruptedException ie) {
			// ignore
		} catch (BKException e) {
			// LOG.error(e.toString());
			// Should return an error status, need to map BK errors into SFStore
			// errors
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
			// The ledger is already open for read. 
			// Nothing to do
			return BKPConstants.SF_OK;
		}
		
		// Let us check if we have LedgerID
		long lId = lpd.getLedgerId();
		
		try {
			rlh = bk.openLedgerNoRecovery(lId, DigestType.MAC, "foo".getBytes());
		} catch (BKException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return BKPConstants.SF_InternalError;
		}
		elm.getLedgerPrivate(extentId).setReadLedgerHandle(rlh);
		return BKPConstants.SF_OK;
	}
	
	public long LedgerNextEntry(String extentId) {
		return (elm.getLedgerPrivate(extentId).allocateEntryId()); 
	}
	
	public boolean LedgerExists(String extentId){
		return elm.extentExists(extentId);
	}

	public int LedgerStat(String extentId) {
		int ledgerSize;
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
		ledgerSize = (int) lh.getLength();
		return ledgerSize;
	}

	public void LedgerDeleteAll() {
		elm.deleteAllLedgerHandles();
	}
	
	public byte LedgerWriteClose(String extentId) {
		
		if (!elm.extentExists(extentId)){
			// No Extent just return OK
			return BKPConstants.SF_OK;
		}
		
		lh = elm.getLedgerPrivate(extentId).getWriteLedgerHandle();
		
		if (lh == null)
			return BKPConstants.SF_OK;
		
		try {
			lh.close();
			// Reset Ledger Handle
			// TODO  Serialize handle setters and getters
			elm.getLedgerPrivate(extentId).setWriteLedgerHandle(null);
		} catch (InterruptedException | BKException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return BKPConstants.SF_ErrorNotFound;
		}
		return BKPConstants.SF_OK;
	}
	
	
	public byte LedgerReadClose(String extentId) {
		
		if (!elm.extentExists(extentId)){
			// No Extent just return OK
			return BKPConstants.SF_OK;
		}
		
		lh = elm.getLedgerPrivate(extentId).getReadLedgerHandle();
		
		if (lh == null)
			return BKPConstants.SF_OK;
		
		try {
			lh.close();
			// Reset the Ledger Handle
			// TODO: Need to serialize handle get/set
			elm.getLedgerPrivate(extentId).setReadLedgerHandle(null);
		} catch (InterruptedException | BKException e) {
			// TODO Auto-generated catch block
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
		} catch (InterruptedException ie) {
			// ignore
		} catch (BKException e) {
			// LOG.error(e.toString());
			// Should return an error status, need to map BK errors into SFStore
			// errors
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
			// In Good Path:
			//   Rules:
			//      1. All allocated fragments must be committed before committing trailer.
			//      2. FragmentId = EntryId + 1
			//      3. TrailerId = Last entryId of the Ledger.
			//      4. Allow write only if the fragment# is the next in sequence.
			//                    fragmentId == last_committed_entryId + 2
			//      5. Allow trailer only if last_comitted entryId  == max allocated entryId
			
			// Logic: 
			// All writes hold lock to serialize access to BK. BK allows single writer.
			// timedout = 0;
			// while(timeout) {
			//     if (fragmentId == last_comitted_entryId + 2)
			//         break;
			//     sleep(1);
			// }
			// if(timedout)
			//    return SF_OutOfSequenceTimeout;
			// lock()
			// if (fragmentId != last_comitted_entryId + 2) {
			//    unlock()
			//    return SF_OutOfSequenceTimeout;
			// }
			// bkhandle.write();
			// unlock();
			int timeoutLoopCount = BKPConstants.WRITE_TIMEOUT * 1000;
			long wEid = lpd.getLastWriteEntryId();
			long aEid = lpd.getAllocedEntryId();
			while (timeoutLoopCount != 0) {
				
				if (fragmentId == 0){
					// TODO: For now allow one outstanding allocateID as the stream increments
					// next write unconditionally. 
					if ((wEid == aEid) || (wEid == aEid - 1)) // All allocated entries are committed. We can write trailer
						break; // allow write
				} else {
					if ((wEid == BKPConstants.NO_ENTRY) && (fragmentId == 1)) // First write
						break; // allow write
					else {
						if (fragmentId == (wEid + 2)) // We are safe to move ahead with write
							break; // allow write
					}
				}
				// Need to wait until timed out or one of the above conditions are met.
				Thread.sleep(1);
				timeoutLoopCount--;
				wEid = lpd.getLastWriteEntryId();
				aEid = lpd.getAllocedEntryId();
			}
			
			if (timeoutLoopCount == 0) {
				// Return Error
				return BKPConstants.SF_OutOfSequenceTimeout;
			}
			
			// Hold the lock and perform above check again under lock to make
			// sure the above condition is still true.
			lpd.lockLedger();
			try {
				wEid = lpd.getLastWriteEntryId();
				aEid = lpd.getAllocedEntryId();
				if (fragmentId == 0) {
					if (!((wEid == aEid) || (wEid == aEid - 1))) {
						return BKPConstants.SF_OutOfSequenceTimeout;
					}
				} else {
					if ((wEid == BKPConstants.NO_ENTRY) && (fragmentId != 1)) {
						return BKPConstants.SF_OutOfSequenceTimeout;
					} else {
						if (fragmentId != (wEid + 2)) {
							return BKPConstants.SF_OutOfSequenceTimeout;
						}
					}
				}

				entryId = lh.addEntry(bdata.array(), 0, bdata.limit());
				lpd.setLastWriteEntryId(entryId);

				// entryId and FragmentId are off by one.
				// BK starts at entryID 0 and Store assumes it to be 1.

				// Handle Trailer
				if (fragmentId == 0) {
					if (lpd.getTrailerId() != BKPConstants.NO_ENTRY) {
						Thread.dumpStack();
					}
					lpd.setTrailerId(entryId);
				} else {
					if (entryId != (fragmentId - 1)) {
						System.out.println("entryId and fragmentIds are not synced.");
						System.out.println("entryId expected: "
								+ (fragmentId - 1) + " returned by BK: "
								+ entryId);
						Thread.dumpStack();
					}
				}
			} finally {
				lpd.unlockLedger();
			}
		} catch (InterruptedException ie) {
			// ignore
		} catch (BKException e) {
			// LOG.error(e.toString());
			// Should return an error status, need to map BK errors into SFStore
			// errors
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
			lh = lpd.getReadLedgerHandle();
			
			if (lh == null) {
				LedgerOpenRead(extentId);
				lh = lpd.getReadLedgerHandle();
			}

			Enumeration<LedgerEntry> entries = null;

			if (fragmentId == 0) {
				// Trailer
				entryId = (int) lpd.getTrailerId();
			} else { // It is not a trailer
				entryId = fragmentId -1;
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
			//cByteBuffer.flip();

		} catch (InterruptedException ie) {
			Thread.dumpStack();
		} catch (BKException bke) {
			// LOG.error(bke.toString());
			return null;
		}
		return cByteBuffer;
	}
}
