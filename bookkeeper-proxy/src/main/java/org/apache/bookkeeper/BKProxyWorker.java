package org.apache.bookkeeper;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.conf.BookKeeperProxyConfiguraiton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class BKProxyWorker implements Runnable {
    private final BookKeeperProxyConfiguraiton bkpConfig;
    SocketChannel clientChannel;
    BKSfdcClient bksc;
    AtomicInteger globalThreadId;    
    byte reqId = BKPConstants.UnInitialized;
    byte respId = BKPConstants.UnInitialized;
    BKExtentId extentId = new BKExtentIdByteArray();
    private final static Logger LOG = LoggerFactory.getLogger(BKProxyWorker.class);

    public BKProxyWorker(BookKeeperProxyConfiguraiton bkpConfig, AtomicInteger threadId, SocketChannel sSock,
            BookKeeper bk, BKExtentLedgerMap elm) {
        this.bkpConfig = bkpConfig;
        this.clientChannel = sSock;
        this.globalThreadId = threadId;       
        try {
            // To facilitate Data Extents,
            // Set both send-buffer and receive-buffer limits of the socket to 64k.
            this.clientChannel.setOption(java.net.StandardSocketOptions.SO_RCVBUF,
                    bkpConfig.getClientChannelReceiveBufferSize());
            this.clientChannel.setOption(java.net.StandardSocketOptions.SO_SNDBUF,
                    bkpConfig.getClientChannelSendBufferSize());
            this.clientChannel.setOption(java.net.StandardSocketOptions.TCP_NODELAY, bkpConfig.getTCPNoDelay());
        } catch (IOException e) {
            // TODO Auto-generated catch block
            globalThreadId.decrementAndGet();
            e.printStackTrace();
        }
        this.bksc = new BKSfdcClient(bkpConfig, bk, elm);
    }


    private int clientChannelRead(ByteBuffer buf) throws IOException {
        int bytesRead = clientChannel.read(buf);
        LOG.info("clientChannelRead - Read bytes: " + bytesRead + " ThreadId: " + Thread.currentThread().getId() + " req: "
                 + reqRespToString(reqId) + " respId: " + reqRespToString(respId) + " ledgerId: " + extentId.asHexString());
        return bytesRead;
    }

    private void clientChannelWrite(ByteBuffer buf) throws IOException {
        LOG.info("clientChannelWrite writing bytes:" + buf.remaining() + " ThreadId: " + Thread.currentThread().getId() + " req: "
                 + reqRespToString(reqId) + " respId: " + reqRespToString(respId) + " ledgerId: " + extentId.asHexString());
        while (buf.hasRemaining()) {
            clientChannel.write(buf);
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
            LOG.info("Starting thread - " + Thread.currentThread().getId() + " No. of Active Threads - " + globalThreadId.get());

            while (true) {
                req.clear();
                resp.clear();
                bytesRead = 0;
                reqId = BKPConstants.UnInitialized;
                respId = BKPConstants.UnInitialized;

                while (bytesRead >= 0 && bytesRead < req.capacity()) {
                    bytesRead = clientChannelRead(req); // read into buffer.
                }

                if (bytesRead < 0) {
                    LOG.info("Exiting Thread - " + Thread.currentThread().getId() + " No. of Active Threads - " + globalThreadId.get());
                    break;
                }

                req.flip();
                reqId = req.get();
                req.get(extentId.asByteArray());

                LOG.info("Request: {} for extentId: {}", reqRespToString(reqId), extentId.asHexString());

                switch (reqId) {

                case (BKPConstants.LedgerStatReq): {

                    respId = BKPConstants.LedgerStatResp;
                    resp.put(respId);

                    // Check if the extent exists
                    if (!bksc.ledgerMapExists(extentId)) {
                        resp.put(BKPConstants.SF_ErrorNotFound);
                    } else {
                        long lSize = bksc.ledgerStat(extentId);
                        resp.put(BKPConstants.SF_OK);
                        resp.putLong(lSize);
                    }
                    resp.flip();
                    clientChannelWrite(resp);
                    break;
                }

                case (BKPConstants.LedgerListGetReq): {
                    Iterable<Long> iterable = bksc.ledgerList();

                    // Count number of elements in the list.
                    int listCount = 0;
                    for (@SuppressWarnings("unused") Long lId : iterable) {
                        listCount++;
                    }

                    respId = BKPConstants.LedgerListGetResp;
                    resp.put(respId);
                    resp.put(BKPConstants.SF_OK);
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
                    // listCunt number of extents.
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
                    byte ret = bksc.ledgerWriteClose(extentId);
                    respId = BKPConstants.LedgerWriteCloseResp;
                    resp.put(respId);
                    resp.put(ret);
                    resp.flip();
                    clientChannelWrite(resp);
                    break;
                }

                case (BKPConstants.LedgerOpenReadReq): {
                    byte ret = bksc.ledgerNonRecoveryOpenRead(extentId);
                    respId = BKPConstants.LedgerOpenReadResp;
                    resp.put(respId);
                    resp.put(ret);
                    resp.flip();
                    clientChannelWrite(resp);
                    break;
                }

                case (BKPConstants.LedgerOpenRecoverReq): {
                    byte ret = bksc.ledgerRecoveryOpenRead(extentId);
                    respId = BKPConstants.LedgerOpenRecoverResp;
                    resp.put(respId);
                    resp.put(ret);
                    resp.flip();
                    clientChannelWrite(resp);
                    break;
                }

                case (BKPConstants.LedgerReadCloseReq): {
                    byte ret = bksc.ledgerReadClose(extentId);
                    respId = BKPConstants.LedgerReadCloseResp;
                    resp.put(respId);
                    resp.put(ret);
                    resp.flip();
                    clientChannelWrite(resp);
                    break;
                }

                case (BKPConstants.LedgerDeleteReq): {
                    byte ret = bksc.ledgerDelete(extentId);
                    respId = BKPConstants.LedgerDeleteResp;
                    resp.put(respId);
                    resp.put(ret);
                    resp.flip();
                    clientChannelWrite(resp);
                    break;
                }

                case (BKPConstants.LedgerDeleteAllReq): {

                    bksc.ledgerDeleteAll();

                    respId = BKPConstants.LedgerDeleteAllResp;
                    resp.put(respId);
                    resp.put(BKPConstants.SF_OK);
                    resp.flip();

                    clientChannelWrite(resp);
                    break;
                }

                case (BKPConstants.LedgerCreateReq): {

                    byte ret = bksc.ledgerCreate(extentId);

                    respId = BKPConstants.LedgerCreateResp;
                    resp.put(respId);
                    resp.put(ret);
                    resp.flip();

                    clientChannelWrite(resp);

                    break;
                }

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
                        resp.put(BKPConstants.SF_ErrorBadRequest);
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

                    byte ret = bksc.ledgerPutEntry(extentId, fragmentId, cByteBuf);

                    resp.put(ret);
                    resp.flip();

                    clientChannelWrite(resp);
                    break;
                }

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
                    ledgerEntry = bksc.ledgerGetEntry(extentId, fragmentId, bufSize);

                    if (ledgerEntry == null) {
                        resp.put(BKPConstants.SF_ErrorNotFound);
                        resp.flip();

                        clientChannelWrite(resp);

                    } else if (bufSize < ledgerEntry.position()) {
                        resp.put(BKPConstants.SF_ShortREAD);
                        resp.flip();
                        clientChannelWrite(resp);
                    } else {
                        resp.put(BKPConstants.SF_OK);
                        resp.putInt(ledgerEntry.position());
                        resp.flip();

                        clientChannelWrite(resp);

                        ledgerEntry.flip();

                        clientChannelWrite(ledgerEntry);
                    }
                    break;
                }
                default:
                    System.out.println("Invalid command = " + reqId);
                }
            }
            clientChannel.close();
            LOG.info("Ending thread - " + Thread.currentThread().getId() + " No. of Active Threads - " + globalThreadId.get());
            globalThreadId.decrementAndGet();
        } catch (IOException e) {
            System.out.println(e);
        }
    }

    static String reqRespToString(byte req) {
        String lstr;
        switch (req) {
        case (BKPConstants.UnInitialized):
            lstr = "== UNINITIALIZED ==";
            break;
        case (BKPConstants.LedgerStatReq):
            lstr = "LedgerStatReq";
            break;
        case (BKPConstants.LedgerStatResp):
            lstr = "LedgerStatResp";
            break;
        case (BKPConstants.LedgerDeleteReq):
            lstr = "LedgerDeleteReq";
            break;
        case (BKPConstants.LedgerDeleteResp):
            lstr = "LedgerDeleteResp";
            break;
        case (BKPConstants.LedgerCreateReq):
            lstr = "LedgerCreateReq";
            break;
        case (BKPConstants.LedgerCreateResp):
            lstr = "LedgerCreateResp";
            break;
        case (BKPConstants.LedgerWriteCloseReq):
            lstr = "LedgerWriteCloseReq";
            break;
        case (BKPConstants.LedgerWriteCloseResp):
            lstr = "LedgerWriteCloseResp";
            break;
        case (BKPConstants.LedgerOpenRecoverReq):
            lstr = "LedgerOpenRecoverReq";
            break;
        case (BKPConstants.LedgerOpenRecoverResp):
            lstr = "LedgerOpenRecoverResp";
            break;
        case (BKPConstants.LedgerOpenReadReq):
            lstr = "LedgerOpenReadReq";
            break;
        case (BKPConstants.LedgerOpenReadResp):
            lstr = "LedgerOpenReadResp";
            break;
        case (BKPConstants.LedgerWriteEntryReq):
            lstr = "LedgerWriteEntryReq";
            break;
        case (BKPConstants.LedgerWriteEntryResp):
            lstr = "LedgerWriteEntryResp";
            break;
        case (BKPConstants.LedgerReadEntryReq):
            lstr = "LedgerReadEntryReq";
            break;
        case (BKPConstants.LedgerReadEntryResp):
            lstr = "LedgerReadEntryResp";
            break;
        case (BKPConstants.ReservedForFutureReq):
            lstr = "ReservedForFutureReq";
            break;
        case (BKPConstants.ReservedForFutureResp):
            lstr = "ReservedForFutureResp";
            break;
        case (BKPConstants.LedgerReadCloseReq):
            lstr = "LedgerReadCloseReq";
            break;
        case (BKPConstants.LedgerReadCloseResp):
            lstr = "LedgerReadCloseResp";
            break;
        case (BKPConstants.LedgerListGetReq):
            lstr = "LedgerListGetReq";
            break;
        case (BKPConstants.LedgerListGetResp):
            lstr = "LedgerListGetResp";
            break;
        case (BKPConstants.LedgerDeleteAllReq):
            lstr = "LedgerDeleteAllReq";
            break;
        case (BKPConstants.LedgerDeleteAllResp):
            lstr = "LedgerDeleteAllResp";
            break;
        default:
            lstr = "UnKnownRequest/UnknowResponse";
        }
        return lstr;
    }
}
