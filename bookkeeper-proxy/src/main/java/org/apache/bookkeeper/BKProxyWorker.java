package org.apache.bookkeeper;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BKException.Code;
import org.apache.bookkeeper.conf.BookKeeperProxyConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class BKProxyWorker implements Runnable {
    private final static Logger LOG = LoggerFactory.getLogger(BKProxyWorker.class);

    private final BookKeeperProxyConfiguration bkpConfig;
    SocketChannel clientChannel;
    BKSfdcClient bksc;
    AtomicInteger globalThreadId;
    byte reqId = BKPConstants.UnInitialized;
    byte respId = BKPConstants.UnInitialized;
    BKExtentId extentId = new BKExtentIdByteArray();

    public BKProxyWorker(BookKeeperProxyConfiguration bkpConfig, AtomicInteger threadId, SocketChannel sSock,
            BookKeeper bk, BKExtentLedgerMap elm) throws IOException {
        this.bkpConfig = bkpConfig;
        this.clientChannel = sSock;
        this.globalThreadId = threadId;

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
            globalThreadId.decrementAndGet();
            throw e;
        }
        this.bksc = new BKSfdcClient(bkpConfig, bk, elm);
    }

    private int clientChannelRead(ByteBuffer buf) throws IOException {
        int bytesRead = 0;
        try {
            bytesRead = clientChannel.read(buf);
        } catch (IOException e) {
            LOG.error("Exception in read. ThreadId: " + Thread.currentThread().getId() + " req: " +
                    BKPConstants.getReqRespString(reqId) + " respId: " + BKPConstants.getReqRespString(respId)
                    + " ledgerId: " + extentId.asHexString(), e);
            throw e;
        }
        LOG.info("clientChannelRead - Read bytes: " + bytesRead + " ThreadId: " + Thread.currentThread().getId()
                + " req: " + BKPConstants.getReqRespString(reqId) + " respId: " + BKPConstants.getReqRespString(respId)
                 + " ledgerId: " + extentId.asHexString());
        return bytesRead;
    }

    private void clientChannelWrite(ByteBuffer buf) throws IOException {
        LOG.info("clientChannelWrite writing bytes:" + buf.remaining() + " ThreadId: " + Thread.currentThread().getId()
                + " req: " + BKPConstants.getReqRespString(reqId) + " respId: " + BKPConstants.getReqRespString(respId)
                + " ledgerId: " + extentId.asHexString());
        try {
            while (buf.hasRemaining()) {
                clientChannel.write(buf);
            }
        } catch (IOException e) {
            LOG.error("Exception in write. ThreadId: " + Thread.currentThread().getId() + " req: " +
                    BKPConstants.getReqRespString(reqId) + " respId: " + BKPConstants.getReqRespString(respId)
                    + " ledgerId: " + extentId.asHexString(), e);
            throw e;
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

        int bytesRead;
        try {
            LOG.info("Starting thread - " + Thread.currentThread().getId() + " No. of Active Threads - "
                    + globalThreadId.get());

            while (true) {
                byte errorCode = BKPConstants.SF_OK;
                req.clear();
                resp.clear();
                bytesRead = 0;
                reqId = BKPConstants.UnInitialized;
                respId = BKPConstants.UnInitialized;

                while (bytesRead >= 0 && bytesRead < req.capacity()) {
                    bytesRead = clientChannelRead(req); // read into buffer.
                }

                if (bytesRead < 0) {
                    LOG.info("Exiting Thread - " + Thread.currentThread().getId() + " No. of Active Threads - "
                            + globalThreadId.get());
                    break;
                }

                req.flip();
                reqId = req.get();
                req.get(extentId.asByteArray());

                LOG.info("Request: {} for extentId: {}", BKPConstants.getReqRespString(reqId), extentId.asHexString());

                switch (reqId) {

                case (BKPConstants.LedgerStatReq): {
                    errorCode = BKPConstants.SF_OK;
                    respId = BKPConstants.LedgerStatResp;
                    resp.put(respId);

                    long lSize = 0;
                    try {
                        lSize = bksc.ledgerStat(extentId);
                    } catch (BKException e) {
                        if (e.getCode() == Code.NoSuchLedgerExistsException) {
                            errorCode = BKPConstants.SF_ErrorNotFound;
                        } else {
                            LOG.error("Exception when getting statistics for extent {}", extentId.asHexString(), e);
                            errorCode = BKPConstants.SF_InternalError;
                        }
                    } catch (Exception e) {
                        LOG.error("Exception when getting statistics for extent {}", extentId.asHexString(), e);
                        errorCode = BKPConstants.SF_InternalError;
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
                    errorCode = BKPConstants.SF_OK;
                    Iterable<Long> iterable = bksc.ledgerList();

                    // Count number of elements in the list.
                    int listCount = 0;
                    for (@SuppressWarnings("unused") Long lId : iterable) {
                        listCount++;
                    }
                    respId = BKPConstants.LedgerListGetResp;
                    resp.put(respId);
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

                    ByteBuffer bExtentId = ByteBuffer.allocate(BKPConstants.EXTENTID_SIZE);
                    iterable = bksc.ledgerList();
                    for (Long pId : iterable) {
                        bExtentId.clear();
                        bExtentId.putLong(0L);
                        bExtentId.putLong(pId.longValue());
                        bExtentId.flip();
                        clientChannelWrite(bExtentId);
                        listCount--;
                        if (listCount == 0)
                            break;
                    }

                    // Handle the case where extents got deleted after taking listCount.
                    for (int i = 0; i < listCount; i++) {
                        bExtentId.clear();
                        bExtentId.putLong(0L);
                        bExtentId.putLong(0L);
                        bExtentId.flip();
                        clientChannelWrite(bExtentId);
                    }
                    break;
                }

                case (BKPConstants.LedgerWriteCloseReq): {
                    errorCode = bksc.ledgerWriteUnsafeClose(extentId);
                    respId = BKPConstants.LedgerWriteCloseResp;
                    resp.put(respId);
                    resp.put(errorCode);
                    resp.flip();
                    clientChannelWrite(resp);
                    break;
                }

                case (BKPConstants.LedgerWriteSafeCloseReq): {
                    errorCode = bksc.ledgerWriteSafeClose(extentId);
                    respId = BKPConstants.LedgerWriteSafeCloseResp;
                    resp.put(respId);
                    resp.put(errorCode);
                    resp.flip();
                    clientChannelWrite(resp);
                    break;
                }

                case (BKPConstants.LedgerOpenReadReq): {
                    errorCode = bksc.ledgerNonRecoveryOpenRead(extentId);
                    respId = BKPConstants.LedgerOpenReadResp;
                    resp.put(respId);
                    resp.put(errorCode);
                    resp.flip();
                    clientChannelWrite(resp);
                    break;
                }

                case (BKPConstants.LedgerOpenRecoverReq): {
                    errorCode = bksc.ledgerRecoveryOpenRead(extentId);
                    respId = BKPConstants.LedgerOpenRecoverResp;
                    resp.put(respId);
                    resp.put(errorCode);
                    resp.flip();
                    clientChannelWrite(resp);
                    break;
                }

                case (BKPConstants.LedgerReadCloseReq): {
                    errorCode = bksc.ledgerReadUnsafeClose(extentId);
                    respId = BKPConstants.LedgerReadCloseResp;
                    resp.put(respId);
                    resp.put(errorCode);
                    resp.flip();
                    clientChannelWrite(resp);
                    break;
                }

                case (BKPConstants.LedgerReadSafeCloseReq): {
                    errorCode = bksc.ledgerReadSafeClose(extentId);
                    respId = BKPConstants.LedgerReadSafeCloseResp;
                    resp.put(respId);
                    resp.put(errorCode);
                    resp.flip();
                    clientChannelWrite(resp);
                    break;
                }

                case (BKPConstants.LedgerDeleteReq): {
                    errorCode = bksc.ledgerDelete(extentId);
                    respId = BKPConstants.LedgerDeleteResp;
                    resp.put(respId);
                    resp.put(errorCode);
                    resp.flip();
                    clientChannelWrite(resp);
                    break;
                }

                case (BKPConstants.LedgerDeleteAllReq): {
                    errorCode = BKPConstants.SF_OK;
                    bksc.ledgerDeleteAll();

                    respId = BKPConstants.LedgerDeleteAllResp;
                    resp.put(respId);
                    resp.put(errorCode);
                    resp.flip();

                    clientChannelWrite(resp);
                    break;
                }

                case (BKPConstants.LedgerCreateReq): {

                    errorCode = bksc.ledgerCreate(extentId);

                    respId = BKPConstants.LedgerCreateResp;
                    resp.put(respId);
                    resp.put(errorCode);
                    resp.flip();

                    clientChannelWrite(resp);

                    break;
                }

                //AMK: todo
                case (BKPConstants.LedgerWriteEntryReq): {
                    int fragmentId;
                    int wSize;

                    ewreq.clear();
                    bytesRead = 0;
                    while (bytesRead >= 0 && bytesRead < ewreq.capacity()) {
                        bytesRead += clientChannelRead(ewreq);
                    }
                    ewreq.flip();

                    respId = BKPConstants.LedgerWriteEntryResp;
                    // Put the Response out as first step.
                    resp.put(respId);

                    fragmentId = ewreq.getInt();
                    wSize = ewreq.getInt();
                    if (wSize > cByteBuf.capacity()) {
                        errorCode = BKPConstants.SF_ErrorBadRequest;
                        LOG.error("Write message size:{} bigger than allowed:{}", wSize, cByteBuf.capacity());
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
                        // TODO: Throw Exception.
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

                    errorCode = bksc.ledgerPutEntry(extentId, fragmentId, cByteBuf);

                    resp.put(errorCode);
                    resp.flip();

                    clientChannelWrite(resp);
                    break;
                }

                // AMK: todo
                case (BKPConstants.LedgerReadEntryReq): {
                    int fragmentId;
                    int bufSize;
                    ByteBuffer ledgerEntry = null;

                    erreq.clear();
                    bytesRead = 0;
                    while (bytesRead >= 0 && bytesRead < erreq.capacity()) {
                        bytesRead += clientChannelRead(erreq);
                    }
                    erreq.flip();
                    fragmentId = erreq.getInt();
                    bufSize = erreq.getInt();

                    respId = BKPConstants.LedgerReadEntryResp;
                    resp.put(respId);

                    // Now get the fragment/entry
                    errorCode = BKPConstants.SF_OK;
                    try {
                        ledgerEntry = bksc.ledgerGetEntry(extentId, fragmentId, bufSize);
                    } catch (BKException bke) {
                        if (bke.getCode() == Code.LedgerClosedException) {
                            LOG.error("Found closed ledger when reading ledger entry for extentId {}", extentId.asHexString(), bke);
                            errorCode = BKPConstants.SF_ErrorNotFoundClosed;
                        } else if (bke.getCode() != Code.ReadException) {
                            // SDB tries to find the end of the Extent by reading until it gets an error.
                            // Current readEntries() returns BKReadException in this case.
                            // Since it is valid error for us, skip printing error for this error.
                            LOG.error("Read Exception when reading ledger entry for extentId {}", extentId.asHexString(), bke);
                        }
                        ledgerEntry = null;
                    }

                    if (ledgerEntry == null) {
                        if (errorCode == BKPConstants.SF_OK) {
                            errorCode = BKPConstants.SF_ErrorNotFound;
                        }
                        resp.put(errorCode);
                        resp.flip();

                        clientChannelWrite(resp);

                    } else if (bufSize < ledgerEntry.position()) {
                        errorCode = BKPConstants.SF_ShortREAD;
                        resp.put(errorCode);
                        resp.flip();
                        clientChannelWrite(resp);
                    } else {
                        errorCode = BKPConstants.SF_OK;
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
                    LOG.error("Invalid command: " + reqId);
                }

                LOG.info("Request: " + BKPConstants.getReqRespString(reqId) + "Response: " + 
                        BKPConstants.getReqRespString(errorCode) + "for extentId: {}", extentId.asHexString());
            }

            LOG.info("Ending thread - " + Thread.currentThread().getId() + ". Num of Active Threads - " + globalThreadId.get());


        } catch (IOException e) {
            LOG.error("Exception in worker processing:", e);
        } finally {
            try {
                clientChannel.close();
            } catch (IOException e) {
                LOG.error("Exception while closing client channel:", e);
            }
            globalThreadId.decrementAndGet();
        }
    }
}
