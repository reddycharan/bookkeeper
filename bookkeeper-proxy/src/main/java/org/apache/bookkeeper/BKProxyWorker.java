package org.apache.bookkeeper;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SocketChannel;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.ArrayList;
import java.util.List;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.conf.BookKeeperProxyConfiguration;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.LedgerIdFormatter;
import org.apache.bookkeeper.util.MathUtils;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.bookkeeper.BKProxyStats.*;

class BKProxyWorker implements Runnable {

    private final static Logger LOG = LoggerFactory.getLogger(BKProxyWorker.class);

    private final BookKeeperProxyConfiguration bkpConfig;
    SocketChannel clientChannel;
    ZooKeeper zkc;
    BKSfdcClient bksc;
    byte reqId = BKPConstants.UnInitialized;
    byte respId = BKPConstants.UnInitialized;
    byte errorCode = BKPConstants.SF_OK;
    private BKExtentId extentId;
    private final long bkProxyWorkerId; 
    private static final String LEDGERID_FORMATTER_CLASS = "ledgerIdFormatterClass";
    private final LedgerIdFormatter ledgerIdFormatter;
    private final BKExtentIdFactory extentIdFactory;
    private final BKByteBufferPool byteBufPool;
    private CommandExecutor commandExecutor;

	public abstract class OpStatEntry {

        protected OpStatsLogger osl;

        public OpStatEntry(OpStatsLogger osl) {
            this.osl = osl;
        }

        abstract void markSuccess();

        abstract void markFailure();

    }

    public class OpStatEntryValue extends OpStatEntry {

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

    public class OpStatEntryTimer extends OpStatEntry {

        private long startTime;
        private long elapsedTime;

        public OpStatEntryTimer(OpStatsLogger osl, long startTime) {
            super(osl);
            this.startTime = startTime;
        }

        public void markSuccess() {
            elapsedTime = MathUtils.elapsedNanos(this.startTime);
            osl.registerSuccessfulEvent(elapsedTime, TimeUnit.NANOSECONDS);
        }

        public void markFailure() {
            elapsedTime = MathUtils.elapsedNanos(this.startTime);
            osl.registerFailedEvent(elapsedTime, TimeUnit.NANOSECONDS);
        }

        public long getElapsedTime() {
            return elapsedTime;
        }
    }

    //Stats
    private Queue<OpStatEntry> opStatQueue;
    private Queue<OpStatEntry> asyncWriteStatQueue;
    private final Counter proxyWorkerPoolCounter;
    private final OpStatsLogger ledgerCreationTimer;
    private final OpStatsLogger ledgerRecoveryReadTimer;
    private final OpStatsLogger ledgerNonRecoveryReadTimer;
    private final OpStatsLogger ledgerStatTimer;
    private final OpStatsLogger ledgerDeleteAllTimer;
    private final OpStatsLogger ledgerWriteCloseTimer;
    private final OpStatsLogger ledgerReadCloseTimer;
    private final OpStatsLogger ledgerDeletionTimer;
    private final OpStatsLogger ledgerSyncPutTimer;
    private final OpStatsLogger ledgerAsyncPutTimer;
    private final OpStatsLogger ledgerAsyncPutStatusTimer;
    private final OpStatsLogger ledgerGetTimer;
    private final OpStatsLogger ledgerReadHist;
    private final OpStatsLogger ledgerWriteHist;
    private final OpStatsLogger ledgerListGetTimer;

    public static class BKProxyWorkerBuilder {
        private BookKeeperProxyConfiguration bkpConfig;
        private SocketChannel sSock;
        private BookKeeper bk;
        private ZooKeeper zkc;
        private BKExtentLedgerMap elm;
        private BKByteBufferPool byteBufPool;
        private long bkProxyWorkerId;
        private StatsLogger statsLogger;

        public BKProxyWorkerBuilder setBKPConfig(BookKeeperProxyConfiguration bkpConfig) {
            this.bkpConfig = bkpConfig;
            return this;
        }

        public BKProxyWorkerBuilder setClientSocketChannel(SocketChannel sSock) {
            this.sSock = sSock;
            return this;
        }

        public BKProxyWorkerBuilder setBookKeeperClient(BookKeeper bk) {
            this.bk = bk;
            return this;
        }

        public BKProxyWorkerBuilder setZooKeeperClient(ZooKeeper zkc) {
            this.zkc = zkc;
            return this;
        }

        public BKProxyWorkerBuilder setLedgerMap(BKExtentLedgerMap elm) {
            this.elm = elm;
            return this;
        }

        public BKProxyWorkerBuilder setByteBufPool(BKByteBufferPool byteBufPool) {
            this.byteBufPool = byteBufPool;
            return this;
        }

        public BKProxyWorkerBuilder setBKProxyWorkerId(long bkProxyWorkerId) {
            this.bkProxyWorkerId = bkProxyWorkerId;
            return this;
        }

        public BKProxyWorkerBuilder setStatsLogger(StatsLogger statsLogger) {
            this.statsLogger = statsLogger;
            return this;
        }

        public BKProxyWorker build() throws IOException {
            return new BKProxyWorker(bkpConfig, sSock, bk, zkc, elm, byteBufPool, bkProxyWorkerId, statsLogger);
        }
    }

    public static BKProxyWorkerBuilder newBuilder() {
        return new BKProxyWorkerBuilder();
    }

    public BKProxyWorker(BookKeeperProxyConfiguration bkpConfig, SocketChannel sSock, BookKeeper bk, ZooKeeper zkc,
            BKExtentLedgerMap elm, BKByteBufferPool byteBufPool, long bkProxyWorkerId, StatsLogger statsLogger)
            throws IOException {
        this.bkpConfig = bkpConfig;
        this.clientChannel = sSock;
        this.byteBufPool = byteBufPool;
        this.bkProxyWorkerId = bkProxyWorkerId;
        this.ledgerIdFormatter = LedgerIdFormatter.newLedgerIdFormatter(bkpConfig, LEDGERID_FORMATTER_CLASS);
        this.extentIdFactory = new BKExtentIdByteArrayFactory();
        this.opStatQueue = new LinkedList<OpStatEntry>();
        this.proxyWorkerPoolCounter = statsLogger.getCounter(WORKER_POOL_COUNT);
        this.ledgerCreationTimer = statsLogger.getOpStatsLogger(LEDGER_CREATION_TIME);
        this.ledgerRecoveryReadTimer = statsLogger.getOpStatsLogger(LEDGER_RECOVERY_READ_TIME);
        this.ledgerNonRecoveryReadTimer = statsLogger.getOpStatsLogger(LEDGER_NON_RECOVERY_READ_TIME);
        this.ledgerStatTimer = statsLogger.getOpStatsLogger(LEDGER_STAT_TIME);
        this.ledgerDeleteAllTimer = statsLogger.getOpStatsLogger(LEDGER_DELETE_ALL_TIME);
        this.ledgerWriteCloseTimer = statsLogger.getOpStatsLogger(LEDGER_WRITE_CLOSE_TIME);
        this.ledgerReadCloseTimer = statsLogger.getOpStatsLogger(LEDGER_READ_CLOSE_TIME);
        this.ledgerDeletionTimer = statsLogger.getOpStatsLogger(LEDGER_DELETE_TIME);
        this.ledgerSyncPutTimer = statsLogger.getOpStatsLogger(LEDGER_SYNC_PUT_FRAGMENT_TIME);
        this.ledgerAsyncPutTimer = statsLogger.getOpStatsLogger(LEDGER_ASYNC_PUT_FRAGMENT_TIME);
        this.ledgerGetTimer = statsLogger.getOpStatsLogger(LEDGER_GET_FRAGMENT_TIME);
        this.ledgerReadHist = statsLogger.getOpStatsLogger(GET_FRAGMENT_BYTES);
        this.ledgerWriteHist = statsLogger.getOpStatsLogger(PUT_FRAGMENT_BYTES);
        this.ledgerListGetTimer = statsLogger.getOpStatsLogger(LEDGER_LIST_GET_TIME);
        this.ledgerAsyncPutStatusTimer = statsLogger.getOpStatsLogger(LEDGER_ASYNC_PUT_FRAGMENT_STATUS_TIME);

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
        this.zkc = zkc;
        this.bksc = new BKSfdcClient(bkpConfig, bk, elm, byteBufPool, statsLogger);
        this.commandExecutor = new CommandExecutor();
    }

    private int clientChannelRead(ByteBuffer buf, int expectedSize) throws IOException {
        int bytesRead = 0;
        int totalBytesRead = 0;
        while (totalBytesRead < expectedSize) {
            try {
                bytesRead = clientChannel.read(buf);
                if (bytesRead < 0) {
                    // We got an error reading from the channel.
                    // Throw an exception.
                    throw new IOException("Received Channel end error. Socket must have been closed.");
                }
                totalBytesRead += bytesRead;
            } catch (IOException e) {
                LOG.error("Exception in read. BKProxyWorker: " + bkProxyWorkerId
                        + " exception: " + e);
                throw e;
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("clientChannelRead - Read bytes: " + bytesRead
                + " BKProxyWorker: " + bkProxyWorkerId
                + " req: " + BKPConstants.getReqRespString(reqId)
                + " respId: " + BKPConstants.getReqRespString(respId)
                + " ledgerId: " + extentId);
        }
        return totalBytesRead;
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
                    + " exception: " + e);
            throw e;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("clientChannelWrite wrote bytes:" + bytesToWrite
                + " BKProxyWorker: " + bkProxyWorkerId
                + " req: " + BKPConstants.getReqRespString(reqId)
                + " respId: " + BKPConstants.getReqRespString(respId)
                + " ledgerId: " + extentId);
        }
    }

    private void markStats(Queue<OpStatEntry> opStatQueue, boolean isFail) {
        while (!opStatQueue.isEmpty()) {
            OpStatEntry osl = opStatQueue.remove();
            if (isFail) {
                osl.markFailure();
            } else {
                osl.markSuccess();
            }
        }
    }

    private void sendExtentIdList(List<Long> extIds, ByteBuffer respBuf, ByteBuffer listBuf) throws IOException {
        // first send the generic response with the resp type, error code, and length
        // Client will be ready with a buffer(s) that can hold extIds.size()*BKPConstants.EXTENTID_SIZE bytes.
        // If the list is empty, we send 0 as the length of the list and the client
        // will know that it is the end of the list
        respBuf.clear();
        respBuf.put(BKPConstants.LedgerListGetResp);
        respBuf.put(BKPConstants.SF_OK);
        respBuf.putInt(extIds.size());
        respBuf.flip();
        clientChannelWrite(respBuf);

        listBuf.clear();
        for (Long id : extIds) {
            listBuf.putLong(0L);
            listBuf.putLong(id);
        }
        listBuf.flip();
        clientChannelWrite(listBuf);
        extIds.clear();
    }

    public void run() {
        
        ByteBuffer req = ByteBuffer.allocate(BKPConstants.GENERIC_REQ_SIZE);
        ByteBuffer resp = ByteBuffer.allocate(BKPConstants.RESP_SIZE);
        ByteBuffer ewreq = ByteBuffer.allocate(BKPConstants.WRITE_REQ_SIZE);
        ByteBuffer erreq = ByteBuffer.allocate(BKPConstants.READ_REQ_SIZE);
        ByteBuffer asreq = ByteBuffer.allocate(BKPConstants.ASYNC_STAT_REQ_SIZE);
        ByteBuffer extentBbuf = ByteBuffer.allocate(BKPConstants.EXTENTID_SIZE);

        req.order(ByteOrder.nativeOrder());
        resp.order(ByteOrder.nativeOrder());
        ewreq.order(ByteOrder.nativeOrder());
        erreq.order(ByteOrder.nativeOrder());
        asreq.order(ByteOrder.nativeOrder());

        String clientConn = "";
        String reqSpecific = "";
        int fragmentId = -1;
        int timeout = 0;
        int rSize = 0;
        int wSize = 0;


        try {
            clientConn = this.clientChannel.getRemoteAddress().toString();
        } catch(Exception e) {
            LOG.warn("Exception while trying to get client address: ", e);
        }

        try {
            Thread.currentThread().setName(
                    "BKProxyWorker" + bkProxyWorkerId + ":[" + clientConn + "]:");
            if (LOG.isDebugEnabled()) {
                LOG.debug("Starting BKProxyWorkerId - " + bkProxyWorkerId);
            }

            while (!Thread.interrupted()) {
                boolean exceptionOccurred = false;
                long startTime = MathUtils.nowInNano(); // in case exception happens before reading the request
                ByteBuffer bufBorrowed = null;

                try {
                    req.clear();
                    resp.clear();
                    reqId = BKPConstants.UnInitialized;
                    respId = BKPConstants.UnInitialized;
                    fragmentId = -1;
                    timeout = 0;
                    rSize = 0;
                    wSize = 0;

                    reqSpecific = "";

                    // Four Letter cmd for ProxyStatus.
                    // This will work because first 2 bytes of the
                    // BKProxyrequest is protocol version number and the first
                    // two bytes in case of four letter command
                    // will make very high short number or negative short number
                    // (for eg: in 'ruok' command - 0x72 0x75 0x6f 0x6b, first 2
                    // bytes decimal value will be 29301)
                    // So protocol version number will not overlap with first 2
                    // bytes of the four letter command.
                    
                    req.limit(4);
                    clientChannelRead(req, req.remaining());
                    String commandString = new String(req.array(), 0, 4);                     
                    if (commandExecutor.execute(commandString, clientChannel, bkpConfig, zkc)) {
                        break;
                    }
                    req.limit(req.capacity());
                    clientChannelRead(req, req.remaining()); // read into buffer.

                    req.flip();
                    short version = req.getShort();
                    reqId = req.get();
                    extentBbuf.clear();
                    req.get(extentBbuf.array());
                    this.extentId = extentIdFactory.build(extentBbuf, this.ledgerIdFormatter);

                    respId = BKPConstants.getRespId(reqId);
                    if (version != BKPConstants.SFS_CURRENT_VERSION) {
                        LOG.error("Exiting BKProxyWorker: " + bkProxyWorkerId
                                + ". Received request from unsupported client version {}; expecting {}", version,
                                BKPConstants.SFS_CURRENT_VERSION);
                        errorCode = BKPConstants.SF_ErrorUnknownVersion;
                        resp.put(respId);
                        resp.put(errorCode);
                        resp.flip();
                        clientChannelWrite(resp);
                        break;
                    }

                    if (this.extentId.asLong() < 0) {
                        LOG.error(
                                "Exiting BKProxyWorker: " + bkProxyWorkerId
                                        + " Received Request: {} for negative extentId: {}",
                                BKPConstants.getReqRespString(reqId), extentId);
                        errorCode = BKPConstants.SF_ErrorIncorrectParameter;
                        resp.put(respId);
                        resp.put(errorCode);
                        resp.flip();
                        clientChannelWrite(resp);
                        break;
                    }

                    //Set time after client channel read. Do it again below if needed after another read and before a ledger operation
                    startTime = MathUtils.nowInNano();

                    errorCode = BKPConstants.SF_OK;
                    switch (reqId) {
                    case (BKPConstants.LedgerStatReq): {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Request: {} for extentId: {}", BKPConstants.getReqRespString(reqId), extentId);
                        }

                        resp.put(respId);
                        opStatQueue.add(new OpStatEntryTimer(ledgerStatTimer, startTime));

                        LedgerStat ledgerStat = null;
                        ledgerStat = bksc.ledgerStat(extentId);

                        resp.put(errorCode);
                        resp.putLong(ledgerStat.getSize());
                        resp.putLong(ledgerStat.getCtime());

                        resp.flip();
                        clientChannelWrite(resp);
                        break;
                    }

                    case (BKPConstants.LedgerListGetReq): {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Request: {}", BKPConstants.getReqRespString(reqId));
                        }

                        // this response is used only if an exception
                        // happens in the catch clauses below
                        resp.put(respId);
                        opStatQueue.add(new OpStatEntryTimer(ledgerListGetTimer, startTime));
                        Iterable<Long> iterable = bksc.ledgerList();
                        List<Long> extentIdList = new ArrayList<Long>();
                        // To reduce the GC load allocating these here
                        // instead of inside sendExtentIdList
                        ByteBuffer respBuf = ByteBuffer.allocate(BKPConstants.RESP_SIZE);
                        respBuf.order(ByteOrder.nativeOrder());
                        ByteBuffer listBuf = ByteBuffer.allocate(BKPConstants.EXTENTID_SIZE *
                                                               BKPConstants.LEDGER_LIST_BATCH_SIZE);

                        // Iterate over all the ledgers and send them in batches of
                        // LEDGER_LIST_BATCH_SIZE (100). The last batch may contain
                        // less than 100. To indicate the end of transmission, send
                        // a final response with length of 0
                        int batch = 0;
                        for (Long pId : iterable) {
                            if (extentIdList.size() == BKPConstants.LEDGER_LIST_BATCH_SIZE) {
                                if (LOG.isDebugEnabled()) {
                                    LOG.debug(String.format("Sending batch {} of extentIds", batch++));
                                }
                                sendExtentIdList(extentIdList, respBuf, listBuf);
                            }
                            extentIdList.add(pId);
                        }
                        // send any left over extents in the list
                        if (extentIdList.size() > 0) {
                            sendExtentIdList(extentIdList, respBuf, listBuf);
                        }

                        // Since all the data has been sent, send one last
                        // response packet but indicate that the length is
                        // 0 (list should be empty now). This indicates end of transmission
                        sendExtentIdList(extentIdList, respBuf, listBuf);
                        break;
                    }

                    case (BKPConstants.LedgerWriteCloseReq): {
						LOG.info("Request: {} for extentId: {}", BKPConstants.getReqRespString(reqId), extentId);

                        resp.put(respId);
                        opStatQueue.add(new OpStatEntryTimer(ledgerWriteCloseTimer, startTime));

                        bksc.ledgerWriteClose(extentId);
                        resp.put(errorCode);
                        resp.flip();
                        clientChannelWrite(resp);
                        break;
                    }

                    case (BKPConstants.LedgerOpenReadReq): {
						LOG.info("Request: {} for extentId: {}", BKPConstants.getReqRespString(reqId), extentId);

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
						LOG.info("Request: {} for extentId: {}", BKPConstants.getReqRespString(reqId), extentId);

                        resp.put(respId);
                        opStatQueue.add(new OpStatEntryTimer(ledgerRecoveryReadTimer, startTime));

                        bksc.ledgerRecoveryOpenRead(extentId);
                        resp.put(errorCode);
                        resp.flip();
                        clientChannelWrite(resp);
                        break;
                    }

                    case (BKPConstants.LedgerReadCloseReq): {
						LOG.info("Request: {} for extentId: {}", BKPConstants.getReqRespString(reqId), extentId);

                        resp.put(respId);
                        opStatQueue.add(new OpStatEntryTimer(ledgerReadCloseTimer, startTime));

                        bksc.ledgerReadClose(extentId);
                        resp.put(errorCode);
                        resp.flip();
                        clientChannelWrite(resp);
                        break;
                    }

                    case (BKPConstants.LedgerDeleteReq): {
						LOG.info("Request: {} for extentId: {}", BKPConstants.getReqRespString(reqId), extentId);

                        resp.put(respId);
                        opStatQueue.add(new OpStatEntryTimer(ledgerDeletionTimer, startTime));

                        bksc.ledgerDelete(extentId);
                        resp.put(errorCode);
                        resp.flip();
                        clientChannelWrite(resp);
                        break;
                    }

                    case (BKPConstants.LedgerDeleteAllReq): {
						LOG.info("Request: {}", BKPConstants.getReqRespString(reqId));

                        resp.put(respId);
                        opStatQueue.add(new OpStatEntryTimer(ledgerDeleteAllTimer, startTime));

                        bksc.ledgerDeleteAll();
                        resp.put(errorCode);
                        resp.flip();

                        clientChannelWrite(resp);
                        break;
                    }

                    case (BKPConstants.LedgerCreateReq): {                        
						LOG.info("Request: {} for extentId: {}", BKPConstants.getReqRespString(reqId), extentId);

                        resp.put(respId);
                        opStatQueue.add(new OpStatEntryTimer(ledgerCreationTimer, startTime));

                        bksc.ledgerCreate(extentId);
                        resp.put(errorCode);
                        resp.flip();

                        clientChannelWrite(resp);

                        break;
                    }

                    case (BKPConstants.LedgerAsyncWriteStatusReq): {
                        LedgerAsyncWriteStatus laws;
                        respId = BKPConstants.LedgerAsyncWriteStatusResp;
                        resp.put(respId);

                        asreq.clear();
                        clientChannelRead(asreq, asreq.capacity());
                        asreq.flip();

                        fragmentId = asreq.getInt();
                        timeout = asreq.getInt(); // msecs
                        if (LOG.isDebugEnabled()) {
                            reqSpecific = String.format(" Frag No: %d, Timeout: %d", fragmentId, timeout);
                            LOG.debug("Request: {} extentId: {}", BKPConstants.getReqRespString(reqId),
                                      extentId + reqSpecific);
                        }
                        opStatQueue.add(new OpStatEntryTimer(ledgerAsyncPutStatusTimer, startTime));
                        laws = bksc.ledgerAsyncWriteStatus(extentId, fragmentId, timeout);
                        resp.put(BKPConstants.SF_OK); //RC of the request to get the status.
                        resp.put(laws.getResult()); // RC of the actual async IO
                        resp.putLong(laws.getCompletionLatency()); // IO Completion Time.
                        resp.flip();
                        clientChannelWrite(resp);
                        break;
                    }

                    case (BKPConstants.LedgerWriteEntryReq): {
                        ByteBuffer wcByteBuf;

                        resp.put(respId);

                        ewreq.clear();
                        clientChannelRead(ewreq, ewreq.capacity());
                        ewreq.flip();

                        fragmentId = ewreq.getInt();
                        wSize = ewreq.getInt();
                        if (LOG.isDebugEnabled()) {
                            reqSpecific = String.format(" Frag No.: %d, wSize=%d", fragmentId, wSize);
                            LOG.debug("Request: {} extentId: {}", BKPConstants.getReqRespString(reqId),
                                      extentId + reqSpecific);
                        }
                        if (wSize > bkpConfig.getByteBufferPoolBufferSize()) {
                            wcByteBuf = ByteBuffer.allocate(wSize);
                            bufBorrowed = null;
                        } else {
                            bufBorrowed = byteBufPool.borrowBuffer();
                            wcByteBuf = bufBorrowed;
                        }

                        if (wSize > bkpConfig.getMaxFragSize()) {
                            errorCode = BKPConstants.SF_ErrorIncorrectParameter;
                            LOG.error("Write message size:" + wSize + " on Extent:" + extentId +
                                      " is bigger than allowed:" + wcByteBuf.capacity());
                            // It is required to read the oversized
                            // fragment and empty out the clientsocketchannel,
                            // otherwise it would corrupt the clientsocketchannel
                            int bytesToEmpty = wSize;
                            wcByteBuf.clear();
                            while (bytesToEmpty > 0) {
                                bytesToEmpty -= clientChannelRead(wcByteBuf, Math.min(bytesToEmpty, wcByteBuf.capacity()));
                                wcByteBuf.clear();
                            }
                            resp.put(errorCode);
                            resp.flip();
                            clientChannelWrite(resp);
                            break;
                        }
                        wcByteBuf.clear();
                        clientChannelRead(wcByteBuf, wSize);
                        wcByteBuf.flip();

                        //Exclude channel reads above
                        startTime = MathUtils.nowInNano();
                        opStatQueue.add(new OpStatEntryTimer(ledgerSyncPutTimer, startTime));
                        opStatQueue.add(new OpStatEntryValue(ledgerWriteHist, (long) wcByteBuf.limit()));

                        // null param form opStatQueue indicates sync write
                        bksc.ledgerPutEntry(extentId, fragmentId, wcByteBuf, null, bufBorrowed);

                        resp.put(errorCode);
                        resp.flip();

                        clientChannelWrite(resp);

                        break;
                    }

                    case (BKPConstants.LedgerAsyncWriteEntryReq):{
                        ByteBuffer wcByteBuf;
                        respId = BKPConstants.LedgerAsyncWriteEntryResp;

                        resp.put(respId);

                        ewreq.clear();
                        clientChannelRead(ewreq, ewreq.capacity());
                        ewreq.flip();

                        fragmentId = ewreq.getInt();
                        wSize = ewreq.getInt();

                        if (LOG.isDebugEnabled()) {
                            reqSpecific = String.format(" Frag No.: %d, sSize=%d", fragmentId, wSize);
                            LOG.debug("Request: {} extentId: {}", BKPConstants.getReqRespString(reqId),
								extentId + reqSpecific);
                        }
                        if (wSize > bkpConfig.getByteBufferPoolBufferSize()) {
                            wcByteBuf = ByteBuffer.allocate(wSize);
                            bufBorrowed = null;
                        } else {
                            bufBorrowed = byteBufPool.borrowBuffer();
                            wcByteBuf = bufBorrowed;
                        }

                        if (wSize > bkpConfig.getMaxFragSize()) {
                            errorCode = BKPConstants.SF_ErrorIncorrectParameter;
                            LOG.error("Write message size:" + wSize + " on Extent:" + extentId +
                                      " is bigger than allowed:" + wcByteBuf.capacity());
                            // It is required to read the oversized
                            // fragment and empty out the clientsocketchannel,
                            // otherwise it would corrupt the clientsocketchannel
                            int bytesToEmpty = wSize;
                            wcByteBuf.clear();
                            while (bytesToEmpty > 0) {
                                bytesToEmpty -= clientChannelRead(wcByteBuf, Math.min(bytesToEmpty, wcByteBuf.capacity()));
                                wcByteBuf.clear();
                            }
                            resp.put(errorCode);
                            resp.flip();
                            clientChannelWrite(resp);
                            break;
                        }
                        wcByteBuf.clear();
                        clientChannelRead(wcByteBuf, wSize);

                        wcByteBuf.flip();
                        //Exclude channel reads above
                        startTime = MathUtils.nowInNano();
                        asyncWriteStatQueue = new LinkedList<OpStatEntry>();
                        asyncWriteStatQueue.add(new OpStatEntryTimer(ledgerAsyncPutTimer, startTime));
                        asyncWriteStatQueue.add(new OpStatEntryValue(ledgerWriteHist, (long) wcByteBuf.limit()));

                        // send StatQueue for Async write.
                        bksc.ledgerPutEntry(extentId, fragmentId, wcByteBuf, asyncWriteStatQueue, bufBorrowed);

                        // borrowed buffer will be released by asyncWriteStatus
                        bufBorrowed = null;

                        resp.put(errorCode);
                        resp.flip();

                        clientChannelWrite(resp);
                        break;
                    }

                    case (BKPConstants.LedgerReadEntryReq): {
                        ByteBuffer ledgerEntry = null;

                        resp.put(respId);

                        erreq.clear();
                        clientChannelRead(erreq, erreq.capacity());
                        erreq.flip();
                        fragmentId = erreq.getInt();
                        rSize = erreq.getInt();

                        // Exclude the client reads above
                        startTime = MathUtils.nowInNano();

                        if (LOG.isDebugEnabled()) {
                            reqSpecific = String.format(" Frag No.: %d, rSize=%d", fragmentId, rSize);
                            LOG.debug("Request: {} extentId: {}" + BKPConstants.getReqRespString(reqId),
                                      extentId + reqSpecific);
                        }

                        // Now get the fragment/entry
                        ledgerEntry = bksc.ledgerGetEntry(extentId, fragmentId, rSize);
                        opStatQueue.add(new OpStatEntryTimer(ledgerGetTimer, startTime));
                        opStatQueue.add(new OpStatEntryValue(ledgerReadHist, (long) ledgerEntry.position()));

                        if (rSize < ledgerEntry.position()) {
                            errorCode = BKPConstants.SF_ShortRead;
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
                        errorCode = BKPConstants.SF_ErrorIllegalOperation;
                        LOG.error("Invalid command: " + reqId + " On Extent:" + extentId);
                        resp.put(BKPConstants.InvalidResp);
                        resp.put(errorCode);
                        resp.flip();
                        clientChannelWrite(resp);
                    }
                } catch (BKException e) {
                    // We are handling only BKException here to send right error back
                    // to SDB. Any other error is caught by the catch(Exception) outside
                    // the loop, which will close the clientChannel and exit this worker thread.
                    exceptionOccurred = true;
                    errorCode = BKPConstants.convertBKtoSFerror(e.getCode());
                    reqSpecific = String.format(" FragNo.: {}, wSize: {} rSize: {} timeout: {}",
                            fragmentId, wSize, rSize, timeout);
                    LOG.error("BKException on Req: "
                              + BKPConstants.getReqRespString(reqId)
                              + reqSpecific
                              + " ElapsedMicroSecs: " + MathUtils.elapsedMicroSec(startTime)
                              + " Error: {} extentId: {}", errorCode, extentId);
                    LOG.error("Exception: ", e);
                    resp.put(errorCode);
                    resp.flip();
                    clientChannelWrite(resp);
                }

                finally {
                    if (reqId != BKPConstants.LedgerAsyncWriteEntryReq) {
                        // All non AsyncWrites status is updated here.
                        markStats(opStatQueue, exceptionOccurred);
                    } else {
                        // It is LedgerAsyncWriteEntryReq
                        // markStats here only if we fail to submit the request.
                        // A successful asyncWrite stats will get updated on the callback.
                        // in {@link LedgerAsyncWriteStatus#setComplete(int result, long entryId)}
                        if (exceptionOccurred) {
                            markStats(asyncWriteStatQueue, exceptionOccurred);
                        }
                    }

                    // if buffer borrowed, return it back to pool
                    if (bufBorrowed != null) {
                        byteBufPool.returnBuffer(bufBorrowed);
                    }
                }
            }

        } catch (Exception e) {
            LOG.error("Generic Exception: ", e);
            if (reqId != BKPConstants.UnInitialized) {
                reqSpecific = String.format("FragNo.: {}, wSize: {} rSize: {} timeout: {}",
                        fragmentId, wSize, rSize, timeout);

                LOG.error("Exception on Req: "
                        + BKPConstants.getReqRespString(reqId)
                        + reqSpecific
                        + " extentId: {}", extentId);
            }
        } finally {
            proxyWorkerPoolCounter.dec();
            try {
                clientChannel.close();
            } catch (IOException e) {
                LOG.error("Exception while closing client channel: " + e);
            }
            LOG.info("Ending BKProxyWorkerID - "+ bkProxyWorkerId);
        }
    }

}
