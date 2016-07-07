package org.apache.bookkeeper;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SocketChannel;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.conf.BookKeeperProxyConfiguration;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.LedgerIdFormatter;
import org.apache.bookkeeper.util.MathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.bookkeeper.BKProxyStats.PXY_BYTES_GET_FRAGMENT_HIST;
import static org.apache.bookkeeper.BKProxyStats.PXY_BYTES_PUT_FRAGMENT_HIST;
import static org.apache.bookkeeper.BKProxyStats.PXY_LEDGER_CREATION_TIME;
import static org.apache.bookkeeper.BKProxyStats.PXY_LEDGER_DELETE_ALL_TIME;
import static org.apache.bookkeeper.BKProxyStats.PXY_LEDGER_DELETE_TIME;
import static org.apache.bookkeeper.BKProxyStats.PXY_LEDGER_GET_FRAGMENT_TIME;
import static org.apache.bookkeeper.BKProxyStats.PXY_LEDGER_NON_RECOVERY_READ_TIME;
import static org.apache.bookkeeper.BKProxyStats.PXY_LEDGER_PUT_FRAGMENT_TIME;
import static org.apache.bookkeeper.BKProxyStats.PXY_LEDGER_READ_CLOSE_TIME;
import static org.apache.bookkeeper.BKProxyStats.PXY_LEDGER_RECOVERY_READ_TIME;
import static org.apache.bookkeeper.BKProxyStats.PXY_LEDGER_STAT_TIME;
import static org.apache.bookkeeper.BKProxyStats.PXY_LEDGER_WRITE_CLOSE_TIME;
import static org.apache.bookkeeper.BKProxyStats.PXY_WORKER_POOL_COUNT;
import static org.apache.bookkeeper.BKProxyStats.PXY_LEDGER_LIST_GET_TIME;

class BKProxyWorker implements Runnable {

    private final static Logger LOG = LoggerFactory.getLogger(BKProxyWorker.class);

    private final BookKeeperProxyConfiguration bkpConfig;
    SocketChannel clientChannel;
    BKSfdcClient bksc;
    byte reqId = BKPConstants.UnInitialized;
    byte respId = BKPConstants.UnInitialized;
    byte errorCode = BKPConstants.SF_OK;
    private BKExtentId extentId;
    private final long bkProxyWorkerId; 
    private static final String LEDGERID_FORMATTER_CLASS = "ledgerIdFormatterClass";
    private final LedgerIdFormatter ledgerIdFormatter;

	private abstract class OpStatEntry {

		protected OpStatsLogger osl;

		public OpStatEntry(OpStatsLogger osl) {
			this.osl = osl;
		}

		abstract void markSuccess();

		abstract void markFailure();

	}

	private class OpStatEntryValue extends OpStatEntry {
		
		private long value;
		
		public OpStatEntryValue(OpStatsLogger osl, long value) {
			super(osl);
			this.value = value;
		}

		public void markSuccess() {
			osl.registerSuccessfulValue(value);
		}

		public void markFailure() {
			osl.registerFailedValue(value);
		}

	}

	private class OpStatEntryTimer extends OpStatEntry {
		
		private long startTime;
		
		public OpStatEntryTimer(OpStatsLogger osl, long startTime) {
			super(osl);
			this.startTime = startTime;
		}

		public void markSuccess() {
			osl.registerSuccessfulEvent(MathUtils.elapsedNanos(this.startTime),
					TimeUnit.NANOSECONDS);
		}

		public void markFailure() {
			osl.registerFailedEvent(MathUtils.elapsedNanos(this.startTime),
					TimeUnit.NANOSECONDS);
		}

	}

    //Stats
    private Queue<OpStatEntry> opStatQueue;
    private final Counter proxyWorkerPoolCounter;
    private final OpStatsLogger ledgerCreationTimer;
    private final OpStatsLogger ledgerRecoveryReadTimer;
    private final OpStatsLogger ledgerNonRecoveryReadTimer;
    private final OpStatsLogger ledgerStatTimer;
    private final OpStatsLogger ledgerDeleteAllTimer;
    private final OpStatsLogger ledgerWriteCloseTimer;
    private final OpStatsLogger ledgerReadCloseTimer;
    private final OpStatsLogger ledgerDeletionTimer;
    private final OpStatsLogger ledgerPutTimer;
    private final OpStatsLogger ledgerGetTimer;
    private final OpStatsLogger ledgerReadHist;
    private final OpStatsLogger ledgerWriteHist;
    private final OpStatsLogger ledgerListGetTimer;

    public BKProxyWorker(BookKeeperProxyConfiguration bkpConfig, SocketChannel sSock,
            BookKeeper bk, BKExtentLedgerMap elm, long bkProxyWorkerId, StatsLogger statsLogger)
                    throws IOException {
        this.bkpConfig = bkpConfig;
        this.clientChannel = sSock;
        this.bkProxyWorkerId = bkProxyWorkerId;
        this.ledgerIdFormatter = LedgerIdFormatter.newLedgerIdFormatter(bkpConfig, LEDGERID_FORMATTER_CLASS);
        this.opStatQueue = new LinkedList<OpStatEntry>();
        this.proxyWorkerPoolCounter = statsLogger.getCounter(PXY_WORKER_POOL_COUNT);
        this.ledgerCreationTimer = statsLogger.getOpStatsLogger(PXY_LEDGER_CREATION_TIME);
        this.ledgerRecoveryReadTimer = statsLogger.getOpStatsLogger(PXY_LEDGER_RECOVERY_READ_TIME);
        this.ledgerNonRecoveryReadTimer = statsLogger.getOpStatsLogger(PXY_LEDGER_NON_RECOVERY_READ_TIME);
        this.ledgerStatTimer = statsLogger.getOpStatsLogger(PXY_LEDGER_STAT_TIME);
        this.ledgerDeleteAllTimer = statsLogger.getOpStatsLogger(PXY_LEDGER_DELETE_ALL_TIME);
        this.ledgerWriteCloseTimer = statsLogger.getOpStatsLogger(PXY_LEDGER_WRITE_CLOSE_TIME);
        this.ledgerReadCloseTimer = statsLogger.getOpStatsLogger(PXY_LEDGER_READ_CLOSE_TIME);
        this.ledgerDeletionTimer = statsLogger.getOpStatsLogger(PXY_LEDGER_DELETE_TIME);
        this.ledgerPutTimer = statsLogger.getOpStatsLogger(PXY_LEDGER_PUT_FRAGMENT_TIME);
        this.ledgerGetTimer = statsLogger.getOpStatsLogger(PXY_LEDGER_GET_FRAGMENT_TIME);
        this.ledgerReadHist = statsLogger.getOpStatsLogger(PXY_BYTES_GET_FRAGMENT_HIST);
        this.ledgerWriteHist = statsLogger.getOpStatsLogger(PXY_BYTES_PUT_FRAGMENT_HIST);
        this.ledgerListGetTimer = statsLogger.getOpStatsLogger(PXY_LEDGER_LIST_GET_TIME);

        try {
            // To facilitate Data Extents,
            // Set both send-buffer and receive-buffer limits of the socket to 64k.
            this.clientChannel.setOption(java.net.StandardSocketOptions.SO_RCVBUF,
                    this.bkpConfig.getClientChannelReceiveBufferSize());
            this.clientChannel.setOption(java.net.StandardSocketOptions.SO_SNDBUF,
                    this.bkpConfig.getClientChannelSendBufferSize());
            this.clientChannel.setOption(java.net.StandardSocketOptions.TCP_NODELAY, bkpConfig.getTCPNoDelay());
        } catch (IOException e) {
            LOG.error("Exception creating worker: ", e);
            try {
                clientChannel.close();
            } catch (IOException ioe) {
                LOG.error("Exception while closing client channel:", ioe);
            }
            throw e;
        }
        this.bksc = new BKSfdcClient(bkpConfig, bk, elm, statsLogger);
    }

    private int clientChannelRead(ByteBuffer buf) throws IOException {
        int bytesRead = 0;
        try {
            bytesRead = clientChannel.read(buf);
        } catch (IOException e) {
            LOG.error("Exception in read. BKProxyWorker: "
                    + bkProxyWorkerId
                    + " req: " + BKPConstants.getReqRespString(reqId)
                    + " respId: " + BKPConstants.getReqRespString(respId)
                    + " ledgerId: " + extentId, e);
            throw e;
        }

        LOG.debug("clientChannelRead - Read bytes: " + bytesRead
                + " BKProxyWorker: " + bkProxyWorkerId
                + " req: " + BKPConstants.getReqRespString(reqId)
                + " respId: " + BKPConstants.getReqRespString(respId)
                + " ledgerId: " + extentId);
        return bytesRead;
    }

    private void clientChannelWrite(ByteBuffer buf) throws IOException {
        long bytesToWrite = buf.remaining();
        try {
            while (buf.hasRemaining()) {
                clientChannel.write(buf);
            }
        } catch (IOException e) {
            LOG.error("Exception in write. BKProxyWorker: " + bkProxyWorkerId
                    + " bytes remaining: " + buf.remaining()
                    + " bytes written: " + (bytesToWrite - buf.remaining())
                    + " req: " + BKPConstants.getReqRespString(reqId)
                    + " respId: " + BKPConstants.getReqRespString(respId)
                    + " ledgerId: " + extentId, e);
            throw e;
        }

        LOG.debug("clientChannelWrite wrote bytes:" + bytesToWrite
        + " BKProxyWorker: " + bkProxyWorkerId
        + " req: " + BKPConstants.getReqRespString(reqId)
        + " respId: " + BKPConstants.getReqRespString(respId)
        + " ledgerId: " + extentId);
    }
    
    private void markStats(boolean isFail) {
		while (!opStatQueue.isEmpty()) {
			OpStatEntry osl = opStatQueue.remove();
			if (isFail) {
				osl.markFailure();
			} else {
				osl.markSuccess();
			}
		}
	}

    public void run() {
        
        ByteBuffer req = ByteBuffer.allocate(BKPConstants.GENERIC_REQ_SIZE);
        ByteBuffer resp = ByteBuffer.allocate(BKPConstants.RESP_SIZE);
        ByteBuffer ewreq = ByteBuffer.allocate(BKPConstants.WRITE_REQ_SIZE);
        ByteBuffer erreq = ByteBuffer.allocate(BKPConstants.READ_REQ_SIZE);
        ByteBuffer cByteBuf = ByteBuffer.allocate(bkpConfig.getMaxFragSize());

        req.order(ByteOrder.nativeOrder());
        resp.order(ByteOrder.nativeOrder());
        ewreq.order(ByteOrder.nativeOrder());
        erreq.order(ByteOrder.nativeOrder());

        String clientConn = "";
        try {
            clientConn = this.clientChannel.getRemoteAddress().toString();
        } catch(Exception e) {
            LOG.warn("Exception while trying to get client address: ", e);
        }

        int bytesRead;
        try {
            Thread.currentThread().setName(
                    "BKProxyWorker" + bkProxyWorkerId + ":[" + clientConn + "]:");
            LOG.debug("Starting BKProxyWorkerId - " + bkProxyWorkerId);

            while (!Thread.interrupted()) {
				boolean exceptionOccurred = false;
				long startTime;
                try {
                    req.clear();
                    resp.clear();
                    bytesRead = 0;
                    reqId = BKPConstants.UnInitialized;
                    respId = BKPConstants.UnInitialized;

                    while (bytesRead >= 0 && bytesRead < req.capacity()) {
                        bytesRead += clientChannelRead(req); // read into buffer.
                    }

                    if (bytesRead < 0) {
                        LOG.error("Exiting BKProxyWorker: {}. Socket must have closed (bytes read: {})",
                                bkProxyWorkerId, bytesRead);
                        break;
                    }

                    req.flip();
                    reqId = req.get();
                    ByteBuffer bb = ByteBuffer.allocate(BKPConstants.EXTENTID_SIZE);
                    req.get(bb.array());
                    this.extentId = new BKExtentIdByteArrayFactory().build(bb, this.ledgerIdFormatter);

                    LOG.debug("Request: {} for extentId: {}", BKPConstants.getReqRespString(reqId),
                        extentId);
                    //Set time after client channel read. Do it again below if needed after another read and before a ledger operation
                    startTime = MathUtils.nowInNano();
                    errorCode = BKPConstants.SF_OK;
                    switch (reqId) {
                    case (BKPConstants.LedgerStatReq): {
                        respId = BKPConstants.LedgerStatResp;
                        resp.put(respId);
                        opStatQueue.add(new OpStatEntryTimer(ledgerStatTimer, startTime));

                        long lSize = 0;
                        try {
                            lSize = bksc.ledgerStat(extentId);
                        } catch (BKException e) {
                            LOG.error("Exception when getting stats for extent {}",
                                       extentId, e);
							exceptionOccurred = true;
                            errorCode = BKPConstants.convertBKtoSFerror(e.getCode());
                        } catch (Exception e) {
							exceptionOccurred = true;
                            LOG.error("Exception when getting stats for extent {}",
                                extentId, e);
                            errorCode = BKPConstants.SF_ServerInternalError;
                        }

                        resp.put(errorCode);
                        if (errorCode == BKPConstants.SF_OK) {
                            resp.putLong(lSize);
                        }
                        resp.flip();
                        clientChannelWrite(resp);
                        break;
                    }

                    case (BKPConstants.LedgerListGetReq): {
                        respId = BKPConstants.LedgerListGetResp;
                        resp.put(respId);
                        opStatQueue.add(new OpStatEntryTimer(ledgerListGetTimer, startTime));
                        Iterable<Long> iterable = bksc.ledgerList();

                        // Count number of elements in the list.
                        int listCount = 0;
                        for (@SuppressWarnings("unused") Long lId : iterable) {
                            listCount++;
                        }
                        resp.put(errorCode);
                        resp.putInt(listCount);
                        resp.flip();
                        // Write the response back to client.
                        // Client will be ready with a buffer(s)
                        // that can hold listCount*BKPConstants.EXTENTID_SIZE bytes.

                        clientChannelWrite(resp);

                        // Reiterate through the list to put one extent at a time.
                        // Please note that we took one snapshot to get the list and
                        // second snapshot of ZK nodes to actually send it. It is possible
                        // more extents/ledgers got added or deleted in between and receiver
                        // is expected to read listCount*BKPConstants.EXTENTID_SIZE bytes.
                        // Hence we are adopting the following logic:
                        // - If extents were added after taking the listCount, we send at the most
                        // listCount number of extents.
                        // - If extents were deleted after taking the listCount, we send extent#0s.

                        iterable = bksc.ledgerList();
                        for (Long pId : iterable) {
                            BKExtentId bExtentId = new BKExtentIdByteArrayFactory().build(
                                    pId, this.ledgerIdFormatter);
                            clientChannelWrite(bExtentId.asByteBuffer());
                            listCount--;
                            if (listCount == 0)
                                break;
                        }

                        // Handle the case where extents got deleted after taking listCount.
                        BKExtentId bExtentId = new BKExtentIdByteArrayFactory().build(0L, this.ledgerIdFormatter);
                        for (int i = 0; i < listCount; i++) {
                            clientChannelWrite(bExtentId.asByteBuffer());
                        }
                        break;
                    }

                    case (BKPConstants.LedgerWriteCloseReq): {
                        respId = BKPConstants.LedgerWriteCloseResp;
                        errorCode = BKPConstants.SF_OK;
                        resp.put(respId);
                        opStatQueue.add(new OpStatEntryTimer(ledgerWriteCloseTimer, startTime));

                        bksc.ledgerWriteClose(extentId);
                        resp.put(errorCode);
                        resp.flip();
                        clientChannelWrite(resp);
                        break;
                    }

                    case (BKPConstants.LedgerOpenReadReq): {
                        respId = BKPConstants.LedgerOpenReadResp;
                        errorCode = BKPConstants.SF_OK;
                        resp.put(respId);
                        opStatQueue.add(new OpStatEntryTimer(ledgerNonRecoveryReadTimer, startTime));
                        bksc.ledgerNonRecoveryOpenRead(extentId);
                        resp.put(errorCode);
                        resp.flip();
                        clientChannelWrite(resp);
                        break;
                    }

                    case (BKPConstants.LedgerOpenRecoverReq): {
                        respId = BKPConstants.LedgerOpenRecoverResp;
                        errorCode = BKPConstants.SF_OK;
                        resp.put(respId);
                        opStatQueue.add(new OpStatEntryTimer(ledgerRecoveryReadTimer, startTime));

                        bksc.ledgerRecoveryOpenRead(extentId);
                        resp.put(errorCode);
                        resp.flip();
                        clientChannelWrite(resp);
                        break;
                    }

                    case (BKPConstants.LedgerReadCloseReq): {
                        respId = BKPConstants.LedgerReadCloseResp;
                        errorCode = BKPConstants.SF_OK;
                        resp.put(respId);
                        opStatQueue.add(new OpStatEntryTimer(ledgerReadCloseTimer, startTime));

                        bksc.ledgerReadClose(extentId);
                        resp.put(errorCode);
                        resp.flip();
                        clientChannelWrite(resp);
                        break;
                    }

                    case (BKPConstants.LedgerDeleteReq): {
                        respId = BKPConstants.LedgerDeleteResp;
                        errorCode = BKPConstants.SF_OK;
                        resp.put(respId);
                        opStatQueue.add(new OpStatEntryTimer(ledgerDeletionTimer, startTime));

                        bksc.ledgerDelete(extentId);
                        resp.put(errorCode);
                        resp.flip();
                        clientChannelWrite(resp);
                        break;
                    }

                    case (BKPConstants.LedgerDeleteAllReq): {
                        respId = BKPConstants.LedgerDeleteAllResp;
                        errorCode = BKPConstants.SF_OK;
                        resp.put(respId);
                        opStatQueue.add(new OpStatEntryTimer(ledgerDeleteAllTimer, startTime));

                        bksc.ledgerDeleteAll();
                        resp.put(errorCode);
                        resp.flip();

                        clientChannelWrite(resp);
                        break;
                    }

                    case (BKPConstants.LedgerCreateReq): {
                        respId = BKPConstants.LedgerCreateResp;
                        errorCode = BKPConstants.SF_OK;
                        resp.put(respId);
                        opStatQueue.add(new OpStatEntryTimer(ledgerCreationTimer, startTime));

                        bksc.ledgerCreate(extentId);
                        resp.put(errorCode);
                        resp.flip();

                        clientChannelWrite(resp);

                        break;
                    }

                    case (BKPConstants.LedgerWriteEntryReq): {
                        respId = BKPConstants.LedgerWriteEntryResp;
                        errorCode = BKPConstants.SF_OK;
                        int fragmentId;
                        int wSize;

                        resp.put(respId);

                        ewreq.clear();
                        bytesRead = 0;
                        while (bytesRead >= 0 && bytesRead < ewreq.capacity()) {
                            bytesRead += clientChannelRead(ewreq);
                        }
                        ewreq.flip();

                        fragmentId = ewreq.getInt();
                        wSize = ewreq.getInt();
                        if (wSize > cByteBuf.capacity()) {
                            errorCode = BKPConstants.SF_ErrorBadRequest;
                            LOG.error("Write message size:" + wSize + " on Extent:" + extentId +
                                      " is bigger than allowed:" + cByteBuf.capacity());
                            // #W-2763423 it is required to read the oversized
                            // fragment and empty out the clientsocketchannel,
                            // otherwise it would corrupt the clientsocketchannel
                            bytesRead = 0;
                            cByteBuf.clear();
                            while (bytesRead < wSize) {
                                bytesRead += clientChannelRead(cByteBuf);
                                if (!cByteBuf.hasRemaining()) {
                                    cByteBuf.clear();
                                }
                            }
                            cByteBuf.clear();
                            resp.put(errorCode);
                            resp.flip();
                            clientChannelWrite(resp);
                            break;
                        }

                        bytesRead = 0;
                        cByteBuf.clear();
                        while (bytesRead >= 0 && bytesRead < wSize) {
                            bytesRead += clientChannelRead(cByteBuf);
                        }

                        cByteBuf.flip();
                        //Exclude channel reads above
                        startTime = MathUtils.nowInNano();
                        opStatQueue.add(new OpStatEntryTimer(ledgerPutTimer, startTime));
                        opStatQueue.add(new OpStatEntryValue(ledgerWriteHist, (long) cByteBuf.limit()));

						bksc.ledgerPutEntry(extentId, fragmentId, cByteBuf);

                        resp.put(errorCode);
                        resp.flip();

                        clientChannelWrite(resp);
                        break;
                    }

                    case (BKPConstants.LedgerReadEntryReq): {
                        respId = BKPConstants.LedgerReadEntryResp;
                        errorCode = BKPConstants.SF_OK;
                        int fragmentId;
                        int bufSize;
                        ByteBuffer ledgerEntry = null;

                        resp.put(respId);

                        erreq.clear();
                        bytesRead = 0;
                        while (bytesRead >= 0 && bytesRead < erreq.capacity()) {
                            bytesRead += clientChannelRead(erreq);
                        }
                        erreq.flip();
                        fragmentId = erreq.getInt();
                        bufSize = erreq.getInt();

                        //Exclude the client reads above
                        startTime = MathUtils.nowInNano();
                        // Now get the fragment/entry
                        ledgerEntry = bksc.ledgerGetEntry(extentId, fragmentId, bufSize);
                        opStatQueue.add(new OpStatEntryTimer(ledgerGetTimer, startTime));
                        opStatQueue.add(new OpStatEntryValue(ledgerReadHist, (long) ledgerEntry.position()));

                        if (bufSize < ledgerEntry.position()) {
                            errorCode = BKPConstants.SF_ShortREAD;
                            resp.put(errorCode);
                            resp.flip();
                            clientChannelWrite(resp);
                        } else {
                            // errorCode == BKPConstants.SF_OK and things are in good shape.
                            resp.put(errorCode);
                            resp.putInt(ledgerEntry.position());
                            resp.flip();

                            clientChannelWrite(resp);

                            ledgerEntry.flip();

                            clientChannelWrite(ledgerEntry);
                        }
                        break;
                    }

                    default:
                        errorCode = BKPConstants.SF_ErrorBadRequest;
                        LOG.error("Invalid command: " + reqId + " On Extent:" + extentId);
                        resp.put(BKPConstants.InvalidResp);
                        resp.put(errorCode);
                        resp.flip();
                        clientChannelWrite(resp);
                    }

                    LOG.info("Request: " + BKPConstants.getReqRespString(reqId)
                        + "Response: " + BKPConstants.getReqRespString(respId)
                        + "for extentId: {}", extentId);
                } catch (BKException e) {
					exceptionOccurred = true;
                    LOG.error("Exception on Request: {}, extentId {}: ", BKPConstants.getReqRespString(reqId),
                            extentId);
                    LOG.error("Exception: ", e);
                    errorCode = BKPConstants.convertBKtoSFerror(((BKException)e).getCode());
                    resp.put(errorCode);
                    resp.flip();
                    clientChannelWrite(resp);
                } catch (InterruptedException e) {
					exceptionOccurred = true;
                    LOG.error("Exception on Request: {}, extentId {}: ", BKPConstants.getReqRespString(reqId),
                            extentId);
                    LOG.error("Exception: ", e);
                    errorCode = BKPConstants.SF_ErrorServerInterrupt;
                    resp.put(errorCode);
                    resp.flip();
                    clientChannelWrite(resp);
                } catch (Exception e) {
					exceptionOccurred = true;
                    LOG.error("Exception on Request: {}, extentId {}: ", BKPConstants.getReqRespString(reqId),
                            extentId);
                    LOG.error("Exception: ", e);
                }
                finally {
					markStats(exceptionOccurred);
                }
            }

        } catch (IOException e) {
                LOG.error("Exception in worker processing:", e);
        } finally {
			proxyWorkerPoolCounter.dec();
            try {
                clientChannel.close();
            } catch (IOException e) {
                LOG.error("Exception while closing client channel:", e);
            }
            LOG.info("Ending BKProxyWorkerID - "+ bkProxyWorkerId);
        }
    }

}
