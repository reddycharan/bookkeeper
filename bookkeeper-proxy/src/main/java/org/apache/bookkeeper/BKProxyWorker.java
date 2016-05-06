package org.apache.bookkeeper;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SocketChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.conf.BookKeeperProxyConfiguration;
import org.apache.bookkeeper.util.LedgerIdFormatter;

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

    public BKProxyWorker(BookKeeperProxyConfiguration bkpConfig, SocketChannel sSock,
            BookKeeper bk, BKExtentLedgerMap elm, long bkProxyWorkerId) throws IOException {
        this.bkpConfig = bkpConfig;
        this.clientChannel = sSock;
        this.bkProxyWorkerId = bkProxyWorkerId;
        this.ledgerIdFormatter = LedgerIdFormatter.newLedgerIdFormatter(bkpConfig, LEDGERID_FORMATTER_CLASS);

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
        this.bksc = new BKSfdcClient(bkpConfig, bk, elm);
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

                    errorCode = BKPConstants.SF_OK;
                    switch (reqId) {

                    case (BKPConstants.LedgerStatReq): {
                        respId = BKPConstants.LedgerStatResp;
                        resp.put(respId);

                        long lSize = 0;
                        try {
                            lSize = bksc.ledgerStat(extentId);
                        } catch (BKException e) {
                            LOG.error("Exception when getting stats for extent {}",
                                       extentId, e);
                            errorCode = BKPConstants.convertBKtoSFerror(e.getCode());
                        } catch (Exception e) {
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

                        // Now get the fragment/entry
                        ledgerEntry = bksc.ledgerGetEntry(extentId, fragmentId, bufSize);

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
                    LOG.error("Exception on Request: {}, extentId {}: ", BKPConstants.getReqRespString(reqId),
                            extentId);
                    LOG.error("Exception: ", e);
                    errorCode = BKPConstants.convertBKtoSFerror(((BKException)e).getCode());
                    resp.put(errorCode);
                    resp.flip();
                    clientChannelWrite(resp);
                } catch (InterruptedException e) {
                    LOG.error("Exception on Request: {}, extentId {}: ", BKPConstants.getReqRespString(reqId),
                            extentId);
                    LOG.error("Exception: ", e);
                    errorCode = BKPConstants.SF_ErrorServerInterrupt;
                    resp.put(errorCode);
                    resp.flip();
                    clientChannelWrite(resp);
                } catch (Exception e) {
                    LOG.error("Exception on Request: {}, extentId {}: ", BKPConstants.getReqRespString(reqId),
                            extentId);
                    LOG.error("Exception: ", e);
                }
            }

        } catch (IOException e) {
                LOG.error("Exception in worker processing:", e);
        } finally {
            try {
                clientChannel.close();
            } catch (IOException e) {
                LOG.error("Exception while closing client channel:", e);
            }
            LOG.info("Ending BKProxyWorkerID - "+ bkProxyWorkerId);
        }
    }
}
