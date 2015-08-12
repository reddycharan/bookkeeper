package org.apache.bookkeeper;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteOrder;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.Hashtable;

import org.apache.bookkeeper.client.*;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.log4j.Category;
import org.apache.log4j.Logger;
import org.apache.log4j.Priority;

/*
 * This server starts 2 types of threads
 * 1. Single BKWriter that waits on a BlockingQueue to receive commands and send to BK
 * 2. Multiple Worker threads that will serve multiple clients, and write commands to the BLocking Q
 */
public class BKProxyServer {
	private static final Logger logger = Logger.getLogger(BKProxyServer.class.getName());
	
	static ID threadNum;
	static final int EXTENTID_SIZE = 16;

	// TODO
	// Hook up BK client library
	// Add code to watch and restart threads
	// code to restart the process itself
	// add commands req and resps
	// logging and stats

	public static void main(String args[]) throws Exception {
		threadNum = new ID(0);
		ServerSocketChannel serverChannel = ServerSocketChannel.open();
		serverChannel.setOption(java.net.StandardSocketOptions.SO_RCVBUF, 65536);
		serverChannel.socket().bind(new InetSocketAddress(5555));
		SocketChannel sock = null;
//		BKtempLedger bk = new BKtempLedger();
		BKSfdcClient bk = new BKSfdcClient();

		while (true) {
			logger.info(String.format("JV: Waiting for connection... %d, ",threadNum.id));
			sock = serverChannel.accept();

			// Worker thread to handle each backend's write requests
			new Thread(new Worker(threadNum, sock, (Object)bk)).start();

			if (threadNum.id == 1000) // max connections
				break;

			threadNum.id++;
		}
		serverChannel.close();
		logger.info("Stopping proxy...");
	}
}

class ID {
	public int id;

	ID(int id) {
		this.id = id;
	}
}

class Worker implements Runnable {
	SocketChannel clientChannel;
//	BKtempLedger bk;
	BKSfdcClient bk;
	ID threadNum;
	byte reqId = 0;
	byte respId = 0;
	private static final Logger logger = Logger.getLogger(Worker.class);
	

	public Worker(ID threadNum, SocketChannel sSock, 
			Object wtl  ) {
		this.clientChannel = sSock;
		try {
			this.clientChannel.setOption(java.net.StandardSocketOptions.SO_RCVBUF, 70000);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			this.clientChannel.setOption(java.net.StandardSocketOptions.SO_SNDBUF, 70000);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		this.threadNum = threadNum;
//		this.bk = (BKtempLedger)wtl;
		this.bk = (BKSfdcClient)wtl;
	}

	public void run() {
		ByteBuffer req = ByteBuffer.allocate(BKPConstants.GENERIC_REQ_SIZE);
		ByteBuffer resp = ByteBuffer.allocate(BKPConstants.RESP_SIZE);
		resp.order(ByteOrder.nativeOrder());
	

		byte[] extentId = new byte[BKPConstants.EXTENTID_SIZE];
		int bytesRead = 0;

		try {
			logger .info("Starting thread " + threadNum.id);

			while (true) {
				req.clear();
				resp.clear();
				bytesRead = clientChannel.read(req); // read into buffer.

				if (bytesRead == -1) {
					break;
				}

				
				req.flip();
				reqId = req.get();
				for (int i = 0; i < BKPConstants.EXTENTID_SIZE; i++) {
					extentId[i] = req.get();
				}

				logger.info(" Received Request: " + reqId);
				String extentIDstr = new StringBuilder(new String(extentId, 0,
						extentId.length - 1)).toString();
				logger.info("ExtentID: " + extentIDstr);
				
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
						logger.info("Sending Response" + resp.get(0)
								+ ":" + resp.get(1));
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
						logger.info("Sending Response" + resp.get(0)
								+ ":" + resp.get(1));
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
						logger.info("Sending Response");
						clientChannel.write(resp);
					}
					
					logger.info(" Response : " + resp.get(0) + ":"
							+ resp.get(1));
					
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
						logger.info("Sending Response; fragmentId: " + nextFragmentId);
						clientChannel.write(resp);
					}
					break;
				}

				case (BKPConstants.LedgerWriteEntryReq): { 
					int fragmentId;
					int wSize;
					ByteBuffer ereq = ByteBuffer.allocate(BKPConstants.WRITE_REQ_SIZE);
					clientChannel.read(ereq);
					ereq.flip();
					resp.put(BKPConstants.LedgerWriteEntryResp);

					logger.info("ereq: " + ereq.toString());
					
					ereq.order(ByteOrder.nativeOrder());

					fragmentId = ereq.getInt();
					wSize = ereq.getInt();
					
					logger.info("fragmentdD: " + fragmentId + "  "
							+ "Write Size:" + wSize + " ereq: " + ereq);
					
					ByteBuffer ledgerEntry = ByteBuffer.allocate(wSize);
					
					clientChannel.socket().setReceiveBufferSize(wSize);
					clientChannel.configureBlocking(true);
					
					bytesRead = 0;
					
					while(ledgerEntry.position() < wSize)
						bytesRead += clientChannel.read(ledgerEntry);
									
					ledgerEntry.flip();
					logger.info("LedgerEntry: " + ledgerEntry);
					byte ret = bk.LedgerPutEntry(extentIDstr, fragmentId, ledgerEntry);

					resp.put(ret);
					resp.flip();

					while (resp.hasRemaining()) {
						logger.info("Sending Response");
						clientChannel.write(resp);
					}
					logger.info(" Response : " + resp.get(0) + ":"
							+ resp.get(1));
					break;
				}

				case (BKPConstants.LedgerReadEntryReq): {
					int fragmentId;
					int bufSize;
					ByteBuffer ledgerEntry = null;
					ByteBuffer ereq = ByteBuffer.allocate(BKPConstants.READ_REQ_SIZE);
					clientChannel.read(ereq);
					ereq.flip();
					ereq.order(ByteOrder.nativeOrder());
					fragmentId = ereq.getInt();
					bufSize = ereq.getInt();
					
					resp.put(BKPConstants.LedgerReadEntryResp);

					logger.info("Read: fragmentId: " + fragmentId
							+ " bufSize: " + bufSize);

					// Now get the fragment/entry
					ledgerEntry = bk.LedgerGetEntry(extentIDstr, fragmentId, bufSize);

					if (ledgerEntry == null) {
						resp.put(BKPConstants.SF_ErrorNotFound);
						resp.flip();
						logger.info("Read couldn't find fragment: "
										+ resp.get(0) + ":" + resp.get(1));
						
						while (resp.hasRemaining()) {
							clientChannel.write(resp);
						}
						
					} else if(bufSize < ledgerEntry.limit()) {
						resp.put(BKPConstants.SF_ShortREAD);
						resp.flip();
						while (resp.hasRemaining()) {
							logger.info("Sending Response: "
									+ resp.get(0) + ":" + resp.get(1));
							clientChannel.write(resp);
						}
					} else {
						resp.put(BKPConstants.SF_OK);
						resp.putInt(ledgerEntry.position());
						resp.flip();
						logger.info("Resp: " + resp);

						while (resp.hasRemaining()) {
							logger.info("Sending Response: "
									+ resp.get(0) + ":" + resp.get(1) + " : " + resp.getInt(2));
							clientChannel.write(resp);
						}


						//if (ledgerEntry.position() == ledgerEntry.limit())
							ledgerEntry.flip(); // TODO: Why do we need to do this?
						logger.info("Respbuf: " + ledgerEntry);


						while (ledgerEntry.hasRemaining()) {
							logger.info("Sending Response");
							clientChannel.write(ledgerEntry);
						}
					}
					break;
				}
				default:
					logger.info("Invalid command = " + reqId);
				}

			}
			clientChannel.close();
			logger.info("Ending thread " + threadNum.id);
			threadNum.id--;
		} catch (IOException e) {
			logger.info(e);
		}
	}
}
