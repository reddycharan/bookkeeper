package org.apache.bookkeeper;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.bookkeeper.client.BookKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class BKProxyWorker implements Runnable {
    SocketChannel clientChannel;
    BKSfdcClient bksc;
    AtomicInteger globalThreadId;
    final int myThreadNum;
    byte reqId = 0;
    byte respId = 0;
    private final static Logger LOG = LoggerFactory.getLogger(BKProxyWorker.class);

    public BKProxyWorker(AtomicInteger threadId, SocketChannel sSock, BookKeeper bk, BKExtentLedgerMap elm) {
        this.clientChannel = sSock;
        this.globalThreadId = threadId;
        this.myThreadNum = globalThreadId.get();
        try {
            // To facilitate Data Extents,
            // Set both send-buffer and receive-buffer limits of the socket to 64k.
            this.clientChannel.setOption(java.net.StandardSocketOptions.SO_RCVBUF, 65536);
            this.clientChannel.setOption(java.net.StandardSocketOptions.SO_SNDBUF, 65536);
            this.clientChannel.setOption(java.net.StandardSocketOptions.TCP_NODELAY, true);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            globalThreadId.decrementAndGet();
            e.printStackTrace();
        }
        this.bksc = new BKSfdcClient(bk, elm);
    }

    static String reqToString(byte req) {
        String lstr;
        switch (req) {
        case (BKPConstants.LedgerStatReq):
            lstr = "LedgerStatReq";
            break;
        case (BKPConstants.LedgerDeleteReq):
            lstr = "LedgerDeleteReq";
            break;
        case (BKPConstants.LedgerCreateReq):
            lstr = "LedgerCreateReq";
            break;
        case (BKPConstants.LedgerWriteCloseReq):
            lstr = "LedgerWriteCloseReq";
            break;
        case (BKPConstants.LedgerOpenRecoverReq):
            lstr = "LedgerOpenRecoverReq";
            break;
        case (BKPConstants.LedgerOpenReadReq):
            lstr = "LedgerOpenReadReq";
            break;
        case (BKPConstants.LedgerWriteEntryReq):
            lstr = "LedgerWriteEntryReq";
            break;
        case (BKPConstants.LedgerReadEntryReq):
            lstr = "LedgerReadEntryReq";
            break;
        case (BKPConstants.ReservedForFutureReq):
            lstr = "ReservedForFutureReq";
            break;
        case (BKPConstants.LedgerReadCloseReq):
            lstr = "LedgerReadCloseReq";
            break;
        case (BKPConstants.LedgerListGetReq):
            lstr = "LedgerListGetReq";
            break;
        case (BKPConstants.LedgerDeleteAllReq):
            lstr = "LedgerDeleteAllReq";
            break;
        default:
            lstr = "UnKnownRequest";
        }
        return lstr;
    }

    public void run() {
        ByteBuffer req = ByteBuffer.allocate(BKPConstants.GENERIC_REQ_SIZE);
        ByteBuffer resp = ByteBuffer.allocate(BKPConstants.RESP_SIZE);
        ByteBuffer ewreq = ByteBuffer.allocate(BKPConstants.WRITE_REQ_SIZE);
        ByteBuffer erreq = ByteBuffer.allocate(BKPConstants.READ_REQ_SIZE);
        ByteBuffer cByteBuf = ByteBuffer.allocate(BKPConstants.MAX_FRAG_SIZE);

        req.order(ByteOrder.nativeOrder());
        resp.order(ByteOrder.nativeOrder());
        ewreq.order(ByteOrder.nativeOrder());
        erreq.order(ByteOrder.nativeOrder());

        BKExtentId extentId = new BKExtentIdByteArray();
        int bytesRead;

        try {
            System.out.println("Starting thread " + myThreadNum);

            while (true) {
                req.clear();
                resp.clear();
                bytesRead = 0;
                while (bytesRead >= 0 && bytesRead < req.capacity()) {
                    bytesRead = clientChannel.read(req); // read into buffer.
                }

                if (bytesRead < 0) {
                    System.out.println("Exiting Thread: " + myThreadNum);
                    break;
                }

                req.flip();
                reqId = req.get();
                req.get(extentId.asByteArray());

                LOG.debug("Request: {} for extentId: {}", reqToString(reqId), extentId.asHexString());

                switch (reqId) {

                case (BKPConstants.LedgerStatReq): {

                    resp.put(BKPConstants.LedgerStatResp);

                    // Check if the extent exists
                    if (!bksc.ledgerMapExists(extentId)) {
                        resp.put(BKPConstants.SF_ErrorNotFound);
                    } else {
                        long lSize = bksc.ledgerStat(extentId);
                        resp.put(BKPConstants.SF_OK);
                        resp.putLong(lSize);
                    }
                    resp.flip();
                    while (resp.hasRemaining()) {
                        clientChannel.write(resp);
                    }
                    break;
                }

                case (BKPConstants.LedgerListGetReq): {
                    Iterable<Long> iterable = bksc.ledgerList();

                    // Count number of elements in the list.
                    int listCount = 0;
                    for (@SuppressWarnings("unused") Long lId: iterable) {
                        listCount++;
                    }

                    resp.put(BKPConstants.LedgerListGetResp);
                    resp.put(BKPConstants.SF_OK);
                    resp.putInt(listCount);
                    resp.flip();
                    // Write the response back to client.
                    // Client will be ready with a buffer(s)
                    // that can hold listCount*BKPConstants.EXTENTID_SIZE bytes.

                    while (resp.hasRemaining()) {
                        clientChannel.write(resp);
                    }

                    // Reiterate through the list to put one extent at a time.
                    // Please note that we took one snapshot to get the list and
                    // second snapshot of ZK nodes to actually send it. It is possible
                    // more extents/ledgers got added or deleted in between and receiver
                    // is expected to read listCount*BKPConstants.EXTENTID_SIZE bytes.
                    // Hence we are adopting the following logic:
                    //    - If extents were added after taking the listCount, we send at the most
                    //      listCunt number of extents.
                    //    - If extents were deleted after taking the listCount, we send extent#0s.

                    ByteBuffer bExtentId = ByteBuffer.allocate(BKPConstants.EXTENTID_SIZE);
                    iterable = bksc.ledgerList();
                    for (Long pId: iterable) {
                        bExtentId.clear();
                        bExtentId.putLong(0L);
                        bExtentId.putLong(pId.longValue());
                        bExtentId.flip();
                        while (bExtentId.hasRemaining()) {
                            clientChannel.write(bExtentId);
                        }
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
                        while (bExtentId.hasRemaining()) {
                            clientChannel.write(bExtentId);
                        }
                    }
                    break;
                }

                case (BKPConstants.LedgerWriteCloseReq): {
                    byte ret = bksc.ledgerWriteClose(extentId);
                    resp.put(BKPConstants.LedgerWriteCloseResp);
                    resp.put(ret);
                    resp.flip();
                    while (resp.hasRemaining()) {
                        clientChannel.write(resp);
                    }
                    break;
                }

                case (BKPConstants.LedgerOpenReadReq): {
                    byte ret = bksc.ledgerOpenRead(extentId);
                    resp.put(BKPConstants.LedgerOpenReadResp);
                    resp.put(ret);
                    resp.flip();
                    while (resp.hasRemaining()) {
                        clientChannel.write(resp);
                    }
                    break;
                }

                case (BKPConstants.LedgerReadCloseReq): {
                    byte ret = bksc.ledgerReadClose(extentId);
                    resp.put(BKPConstants.LedgerReadCloseResp);
                    resp.put(ret);
                    resp.flip();
                    while (resp.hasRemaining()) {
                        clientChannel.write(resp);
                    }
                    break;
                }

                case (BKPConstants.LedgerDeleteReq): {
                    byte ret = bksc.ledgerDelete(extentId);
                    resp.put(BKPConstants.LedgerDeleteResp);
                    resp.put(ret);
                    resp.flip();
                    while (resp.hasRemaining()) {
                        clientChannel.write(resp);
                    }
                    break;
                }

                case (BKPConstants.LedgerDeleteAllReq): {

                    bksc.ledgerDeleteAll();

                    resp.put(BKPConstants.LedgerDeleteAllResp);
                    resp.put(BKPConstants.SF_OK);
                    resp.flip();

                    while (resp.hasRemaining()) {
                        clientChannel.write(resp);
                    }
                    break;
                }

                case (BKPConstants.LedgerCreateReq): {

                    byte ret = bksc.ledgerCreate(extentId);

                    resp.put(BKPConstants.LedgerCreateResp);
                    resp.put(ret);
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
                    bytesRead = 0;
                    while (bytesRead >= 0 && bytesRead < ewreq.capacity()) {
                        bytesRead += clientChannel.read(ewreq);
                    };
                    ewreq.flip();

                    // Put the Response out as first step.
                    resp.put(BKPConstants.LedgerWriteEntryResp);

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
                            bytesRead += clientChannel.read(cByteBuf);
                            if (!cByteBuf.hasRemaining()) {
                                cByteBuf.clear();
                            }
                        }
                        cByteBuf.clear();
                        // TODO: Throw Exception.
                        resp.put(BKPConstants.SF_ErrorBadRequest);
                        resp.flip();
                        while (resp.hasRemaining()) {
                            clientChannel.write(resp);
                        }
                        break;
                    }

                    bytesRead = 0;
                    cByteBuf.clear();
                    while (bytesRead >= 0 && bytesRead < wSize) {
                        bytesRead += clientChannel.read(cByteBuf);
                    }

                    cByteBuf.flip();

                    byte ret = bksc.ledgerPutEntry(extentId, fragmentId, cByteBuf);

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
                    bytesRead = 0;
                    while (bytesRead >= 0 && bytesRead < erreq.capacity()) {
                        bytesRead += clientChannel.read(erreq);
                    }
                    erreq.flip();
                    fragmentId = erreq.getInt();
                    bufSize = erreq.getInt();

                    resp.put(BKPConstants.LedgerReadEntryResp);

                    // Now get the fragment/entry
                    ledgerEntry = bksc.ledgerGetEntry(extentId, fragmentId, bufSize);

                    if (ledgerEntry == null) {
                        resp.put(BKPConstants.SF_ErrorNotFound);
                        resp.flip();

                        while (resp.hasRemaining()) {
                            clientChannel.write(resp);
                        }

                    } else if (bufSize < ledgerEntry.position()) {
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
