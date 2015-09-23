package org.apache.bookkeeper;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.bookkeeper.client.BookKeeper;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
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
            // Set both send-buffer and receive-buffer limits of the socket to
            // 64k.
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

    static void reqToString(byte req) {
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
        case (BKPConstants.LedgerNextEntryIdReq):
            lstr = "LedgerNextEntryIdReq";
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
        System.out.println(lstr);
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

        byte[] extentId = new byte[BKPConstants.EXTENTID_SIZE];
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
                req.get(extentId);

                // System.out.println(" Received Request: " + reqId);
                String extentIDstr = Hex.encodeHexString(extentId);
                // System.out.print("Request: " );
                // reqToString(reqId);
                // System.out.println(" ExtentID: " + extentIDstr);
                //
                switch (reqId) {

                case (BKPConstants.LedgerStatReq): {

                    resp.put(BKPConstants.LedgerStatResp);

                    // Check if the extent exists
                    if (!bksc.LedgerExists(extentIDstr)) {                        
                        resp.put(BKPConstants.SF_ErrorNotFound);
                    } else {
                        long lSize = bksc.LedgerStat(extentIDstr);
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
                    String[] extentIds = bksc.LedgerList();

                    resp.put(BKPConstants.LedgerListGetResp);
                    resp.put(BKPConstants.SF_OK);
                    resp.putInt(extentIds.length);
                    resp.flip();

                    while (resp.hasRemaining()) {
                        clientChannel.write(resp);
                    }

                    int esize = extentIds.length * BKPConstants.EXTENTID_SIZE;
                    ByteBuffer ebuf = ByteBuffer.allocate(esize);
                    for (int i = 0; i < extentIds.length; i++) {
                        byte[] ebytes;
                        try {
                            ebytes = Hex.decodeHex(extentIds[i].toCharArray());
                        } catch (DecoderException de) {
                            throw new IOException(de);
                        }
                        ebuf.put(ebytes);
                    }

                    ebuf.flip();
                    while (ebuf.hasRemaining()) {
                        clientChannel.write(ebuf);
                    }

                    break;
                }

                case (BKPConstants.LedgerWriteCloseReq): {
                    byte ret = bksc.LedgerWriteClose(extentIDstr);
                    resp.put(BKPConstants.LedgerWriteCloseResp);
                    resp.put(ret);
                    resp.flip();
                    while (resp.hasRemaining()) {
                        clientChannel.write(resp);
                    }
                    break;
                }

                case (BKPConstants.LedgerOpenReadReq): {
                    byte ret = bksc.LedgerOpenRead(extentIDstr);
                    resp.put(BKPConstants.LedgerOpenReadResp);
                    resp.put(ret);
                    resp.flip();
                    while (resp.hasRemaining()) {
                        clientChannel.write(resp);
                    }
                    break;
                }

                case (BKPConstants.LedgerReadCloseReq): {
                    byte ret = bksc.LedgerReadClose(extentIDstr);
                    resp.put(BKPConstants.LedgerReadCloseResp);
                    resp.put(ret);
                    resp.flip();
                    while (resp.hasRemaining()) {
                        clientChannel.write(resp);
                    }
                    break;
                }

                case (BKPConstants.LedgerDeleteReq): {
                    byte ret = bksc.LedgerDelete(extentIDstr);
                    resp.put(BKPConstants.LedgerDeleteResp);
                    resp.put(ret);
                    resp.flip();
                    while (resp.hasRemaining()) {
                        clientChannel.write(resp);
                    }
                    break;
                }

                case (BKPConstants.LedgerDeleteAllReq): {

                    bksc.LedgerDeleteAll();

                    resp.put(BKPConstants.LedgerDeleteAllResp);
                    resp.put(BKPConstants.SF_OK);
                    resp.flip();

                    while (resp.hasRemaining()) {
                        clientChannel.write(resp);
                    }
                    break;
                }

                case (BKPConstants.LedgerCreateReq): {

                    byte ret = bksc.LedgerCreate(extentIDstr);

                    resp.put(BKPConstants.LedgerCreateResp);
                    resp.put(ret);
                    resp.flip();

                    while (resp.hasRemaining()) {
                        clientChannel.write(resp);
                    }

                    break;
                }

                case (BKPConstants.LedgerNextEntryIdReq): {
                    int nextFragmentId;
                    resp.put(BKPConstants.LedgerNextEntryIdResp);

                    if (bksc.LedgerExists(extentIDstr)) {
                        // FragmentId = EntryId + 1
                        nextFragmentId = (int) bksc.LedgerNextEntry(extentIDstr) + 1;
                        resp.put(BKPConstants.SF_OK);
                        resp.putInt(nextFragmentId);
                    } else {
                        resp.put(BKPConstants.SF_ErrorNotFound);
                    }
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

                    byte ret = bksc.LedgerPutEntry(extentIDstr, fragmentId, cByteBuf);

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
                    ;
                    erreq.flip();
                    fragmentId = erreq.getInt();
                    bufSize = erreq.getInt();

                    resp.put(BKPConstants.LedgerReadEntryResp);

                    // Now get the fragment/entry
                    ledgerEntry = bksc.LedgerGetEntry(extentIDstr, fragmentId, bufSize);

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
