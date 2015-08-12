package org.apache.bookkeeper;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Enumeration;
import java.util.Set;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.zookeeper.KeeperException;

public class BKSfdcClient {
	final String server = "localhost:2181";
	BKExtentLedgerMap elm = new BKExtentLedgerMap();
	long ledgerId = LedgerHandle.INVALID_ENTRY_ID;
	BookKeeper bk = null;
	LedgerHandle lh = null;
	LedgerEntry ledgerEntry = null;
	Object ledgerObj = null;
	boolean exists = false;

	private void setUp() {
		try {
			bk = new BookKeeper(server);
		} catch (InterruptedException ie) {
			// ignore
		} catch (KeeperException | IOException e) {
			// LOG.error(e.toString());
		}
	}

	private void tearDown() {
		try {
			bk.close();
		} catch (InterruptedException ie) {
			// ignore
		} catch (BKException e) {
			// LOG.error(e.toString());
		}
	}

	public byte LedgerCreate(String extentId) {
		try {
			if (bk == null)
				setUp();

			if (elm.extentExists(extentId))
				return BKPConstants.SF_ErrorExist;

			lh = bk.createLedger(3, 2, DigestType.MAC, "foo".getBytes());
			elm.createLedger(extentId, lh);

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
	
	public long LedgerNextEntry(String extentId) {
		
		return (elm.getLedgerPrivate(extentId).allocateEntryId()); 
		
	}

	public boolean LedgerStat(String extentId) {
		if (bk == null)
			setUp();

		return elm.extentExists(extentId);
	}

	public void LedgerDeleteAll() {
		elm.deleteAllLedgerHandles();
	}

	public byte LedgerDelete(String extentId) {

		try {
			if (bk == null)
				setUp();

			lh = elm.getLedgerPrivate(extentId).getLedgerHandle();
			bk.deleteLedger(lh.getId());
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
		byte[] data = new byte[bdata.capacity()];

		try {
			if (bk == null)
				setUp();

			exists = elm.extentExists(extentId);
			if (exists == false) {
				System.out.println(" Add entry: does not exist, extentId: "
						+ extentId.toString());
				return BKPConstants.SF_InternalError;
			}
			
			LedgerPrivateData lpd = elm.getLedgerPrivate(extentId);

			lh = lpd.getLedgerHandle();
			System.out.println(" Ledger Handle: " + lh.getId());

			bdata.get(data, 0, data.length);
			System.out.println("data.length: " + data.length);
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
				System.out.println("############# OutOfSequenceTimeout before lock");
				System.out.println("aEid: " + aEid + " wEid: " + wEid + " FragmentId: " + fragmentId);
				return BKPConstants.SF_OutOfSequenceTimeout;
			} else
				System.out.println("********** Passed New test");
			
			// Hold the lock and perform above check again under lock to make sure.
			lpd.lockLedger();
			wEid = lpd.getLastWriteEntryId();
			aEid = lpd.getAllocedEntryId();
			if (fragmentId == 0) { 
				if (!((wEid == aEid) || (wEid == aEid -1))) {
					System.out.println("aEid: " + aEid + " wEid: " + wEid + " FragmentId: " + fragmentId);
					lpd.unlockLedger();
					return BKPConstants.SF_OutOfSequenceTimeout;
				}
			} else {
				if ((wEid == BKPConstants.NO_ENTRY) && (fragmentId != 1)) {
					lpd.unlockLedger();
					return BKPConstants.SF_OutOfSequenceTimeout;
				} else {
					if (fragmentId != (wEid + 2)) {
						lpd.unlockLedger();
						return BKPConstants.SF_OutOfSequenceTimeout;
					}
				}
			}
			
			entryId = lh.addEntry(data, 0, data.length);
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
				if (entryId != (fragmentId -1)) {
					System.out.println("entryId and fragmentIds are not synced.");
					System.out.println("entryId expected: " + (fragmentId - 1) + 
							" returned by BK: " + entryId);
					Thread.dumpStack();
				}
			}
			lpd.unlockLedger();

			System.out.println(" fragID: " + fragmentId + " entryId: "
					+ entryId);

			// TODO Assert that the ledgerEntry returned is the
			// same as the entryId passed to us.
			// assert(entry == fragmentId)
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
		ByteBuffer bdata = ByteBuffer.allocate(size);
		byte[] data = new byte[size];

		try {
			if (bk == null)
				setUp();

			exists = elm.extentExists(extentId);
			if (exists == false) {
				System.out.println(" Read entry: does not exist, extentId: "
						+ extentId.toString());
				return null;
			}
			
			LedgerPrivateData lpd = elm.getLedgerPrivate(extentId);
			lh = lpd.getLedgerHandle();
			System.out.println(" READ Ledger Handle: " + lh.getId());


			Enumeration<LedgerEntry> entries = null;

			if (fragmentId == 0) {
				// Trailer
				entryId = (int) lpd.getTrailerId();
				System.out.println("Trailer entryId: " + entryId);
			} else { // It is not a trailer
				entryId = fragmentId -1;
				if (entryId == lpd.getTrailerId()) {
					// user can't refer trailer with fragmentId. return NULL
					return null;
				}
			}
			System.out.println("fragmentId: " + fragmentId + " entryId: " + entryId);

			entries = lh.readEntries(entryId, entryId);
			LedgerEntry e = entries.nextElement();

			data = e.getEntry();
			System.out.println("Data: " + data.length + " bData: " + bdata);
			bdata.put(data, 0, Math.min(data.length, bdata.remaining()));
			System.out.println("bdata: " + bdata);

		} catch (InterruptedException ie) {
			// ignore
		} catch (BKException bke) {
			// LOG.error(bke.toString());
			return null;
		}
		return bdata;
	}
}
