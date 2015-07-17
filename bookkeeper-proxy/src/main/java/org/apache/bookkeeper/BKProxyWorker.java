package org.apache.bookkeeper;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicInteger;

class BKProxyWorker implements Runnable {
	SocketChannel clientChannel;
//	BKtempLedger bk;
	BKSfdcClient bk;
	AtomicInteger globalThreadId;
	final int myThreadNum;
	byte reqId = 0;
	byte respId = 0;
	

	public BKProxyWorker(AtomicInteger threadId, SocketChannel sSock, Object wtl  ) {
		this.clientChannel = sSock;
		this.globalThreadId = threadId;
		this.myThreadNum = globalThreadId.get();
		try {
			// To facilitate Data Extents,
			// Set both send-buffer and receive-buffer limits of the socket to 64k.
			this.clientChannel.setOption(java.net.StandardSocketOptions.SO_RCVBUF, 65536);
			this.clientChannel.setOption(java.net.StandardSocketOptions.SO_SNDBUF, 65536);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			globalThreadId.decrementAndGet();
			e.printStackTrace();
		}
//		this.bk = (BKtempLedger)wtl;
		this.bk = (BKSfdcClient)wtl;
	}

	public void run() {
		ByteBuffer req = ByteBuffer.allocate(BKPConstants.GENERIC_REQ_SIZE);
		ByteBuffer resp = ByteBuffer.allocate(BKPConstants.RESP_SIZE);
		ByteBuffer ewreq = ByteBuffer.allocate(BKPConstants.WRITE_REQ_SIZE);
		ByteBuffer erreq = ByteBuffer.allocate(BKPConstants.READ_REQ_SIZE);

		req.order(ByteOrder.nativeOrder());
		resp.order(ByteOrder.nativeOrder());
		ewreq.order(ByteOrder.nativeOrder());
		erreq.order(ByteOrder.nativeOrder());


		byte[] extentId = new byte[BKPConstants.EXTENTID_SIZE];
		int bytesRead = 0;

		try {
			System.out.println("Starting thread " + myThreadNum);

			while (true) {
				req.clear();
				resp.clear();
				bytesRead = clientChannel.read(req); // read into buffer.

				if (bytesRead < 0) {
					System.out.println("Exiting Thread: " + myThreadNum );
					break;
				}

				req.flip();
				reqId = req.get();
				req.get(extentId);

//				System.out.println(" Received Request: " + reqId);
				String extentIDstr = new StringBuilder(new String(extentId, 0,
						extentId.length - 1)).toString();
//				System.out.println("ExtentID: " + extentIDstr);
				
				switch (reqId) {

				case (BKPConstants.LedgerStatReq): { 
					resp.put(BKPConstants.LedgerStatResp);
					if (bk.LedgerStat(extentIDstr)) {
						resp.put(BKPConstants.SF_OK);
					} else {
						resp.put(BKPConstants.SF_ErrorNotFound);
					}
					resp.flip();
					while (resp.hasRemaining()) {
						clientChannel.write(resp);
					}
					break;
				}
				
				case (BKPConstants.LedgerWriteCloseReq): {
					byte ret = bk.LedgerWriteClose(extentIDstr);
					resp.put(BKPConstants.LedgerWriteCloseResp);
					resp.put(ret);
					resp.flip();
					while (resp.hasRemaining()) {
						clientChannel.write(resp);
					}
					break;
				}
				
				case (BKPConstants.LedgerOpenReadReq): {
					byte ret = bk.LedgerOpenRead(extentIDstr);
					resp.put(BKPConstants.LedgerOpenReadResp);
					resp.put(ret);
					resp.flip();
					while (resp.hasRemaining()) {
						clientChannel.write(resp);
					}
					break;
				}
				
				case (BKPConstants.LedgerReadCloseReq): {
					byte ret = bk.LedgerReadClose(extentIDstr);
					resp.put(BKPConstants.LedgerReadCloseResp);
					resp.put(ret);
					resp.flip();
					while (resp.hasRemaining()) {
						clientChannel.write(resp);
					}
					break;
				}
				
				case (BKPConstants.LedgerDeleteReq): {
					byte ret = bk.LedgerDelete(extentIDstr);
					resp.put(BKPConstants.LedgerDeleteResp);
					resp.put(ret);
					resp.flip();
					while (resp.hasRemaining()) {
						clientChannel.write(resp);
					}
					break;
				}
			
				case (BKPConstants.LedgerDeleteAllReq): { 
					
					bk.LedgerDeleteAll();

					resp.put(BKPConstants.LedgerDeleteAllResp);
					resp.put(BKPConstants.SF_OK);
					resp.flip();
					
					while (resp.hasRemaining()) {
						clientChannel.write(resp);
					}
					break;
				}

				case (BKPConstants.LedgerCreateReq): { 
					
					byte ret = bk.LedgerCreate(extentIDstr);
				
					resp.put(BKPConstants.LedgerCreateResp);
					resp.put(ret);
					resp.flip();
					
					while (resp.hasRemaining()) {
						clientChannel.write(resp);
					}
					
					break;
				}
				
				case (BKPConstants.LedgerNextEntryIdReq): {
					// FragmentId = EntryId + 1
					int nextFragmentId = (int) bk.LedgerNextEntry(extentIDstr) + 1;
					resp.put(BKPConstants.LedgerNextEntryIdResp);
					resp.put(BKPConstants.SF_OK);
					resp.putInt(nextFragmentId);
					resp.flip();
					while (resp.hasRemaining()) {
						clientChannel.write(resp);
					}
					break;
				}

				case (BKPConstants.LedgerWriteEntryReq): { 
					int fragmentId;
					int wSize;
					
					ewreq.clear();
					clientChannel.read(ewreq);
					ewreq.flip();
					
					// Put the Response out as first step.
					resp.put(BKPConstants.LedgerWriteEntryResp);

					fragmentId = ewreq.getInt();
					wSize = ewreq.getInt();
					
					ByteBuffer ledgerEntry = ByteBuffer.allocate(wSize);
					
					bytesRead = 0;
					
					while(ledgerEntry.position() < wSize)
						bytesRead += clientChannel.read(ledgerEntry);
									
					ledgerEntry.flip();
					
					byte ret = bk.LedgerPutEntry(extentIDstr, fragmentId, ledgerEntry);

					resp.put(ret);
					resp.flip();

					while (resp.hasRemaining()) {
						clientChannel.write(resp);
					}
					break;
				}

				case (BKPConstants.LedgerReadEntryReq): {
					int fragmentId;
					int bufSize;
					ByteBuffer ledgerEntry = null;
					
					erreq.clear();
					clientChannel.read(erreq);
					erreq.flip();
					fragmentId = erreq.getInt();
					bufSize = erreq.getInt();
					
					resp.put(BKPConstants.LedgerReadEntryResp);

					// Now get the fragment/entry
					ledgerEntry = bk.LedgerGetEntry(extentIDstr, fragmentId, bufSize);

					if (ledgerEntry == null) {
						resp.put(BKPConstants.SF_ErrorNotFound);
						resp.flip();
						
						while (resp.hasRemaining()) {
							clientChannel.write(resp);
						}
						
					} else if(bufSize < ledgerEntry.limit()) {
						resp.put(BKPConstants.SF_ShortREAD);
						resp.flip();
						while (resp.hasRemaining()) {
							clientChannel.write(resp);
						}
					} else {
						resp.put(BKPConstants.SF_OK);
						resp.putInt(ledgerEntry.position());
						resp.flip();

						while (resp.hasRemaining()) {
							clientChannel.write(resp);
						}

						ledgerEntry.flip(); 

						while (ledgerEntry.hasRemaining()) {
							clientChannel.write(ledgerEntry);
						}
					}
					break;
				}
				default:
					System.out.println("Invalid command = " + reqId);
				}
			}
			clientChannel.close();
			System.out.println("Ending thread " + myThreadNum);
			globalThreadId.decrementAndGet();
		} catch (IOException e) {
			System.out.println(e);
		}
	}
}
