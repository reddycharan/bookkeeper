/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bookkeeper.proto;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.DefaultChannelPromise;
import io.netty.channel.DefaultEventLoopGroup;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.local.LocalChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.CorruptedFrameException;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.codec.TooLongFrameException;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.bookkeeper.auth.ClientAuthProvider;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeperClientStats;
import org.apache.bookkeeper.client.BookieInfoReader.BookieInfo;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.ReadEntryCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.ReadLacCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GetBookieInfoCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteLacCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.StartTLSCallback;
import org.apache.bookkeeper.proto.BookkeeperProtocol.AddRequest;
import org.apache.bookkeeper.proto.BookkeeperProtocol.AddResponse;
import org.apache.bookkeeper.proto.BookkeeperProtocol.BKPacketHeader;
import org.apache.bookkeeper.proto.BookkeeperProtocol.GetBookieInfoRequest;
import org.apache.bookkeeper.proto.BookkeeperProtocol.GetBookieInfoResponse;
import org.apache.bookkeeper.proto.BookkeeperProtocol.OperationType;
import org.apache.bookkeeper.proto.BookkeeperProtocol.ProtocolVersion;
import org.apache.bookkeeper.proto.BookkeeperProtocol.ReadLacRequest;
import org.apache.bookkeeper.proto.BookkeeperProtocol.ReadLacResponse;
import org.apache.bookkeeper.proto.BookkeeperProtocol.ReadRequest;
import org.apache.bookkeeper.proto.BookkeeperProtocol.ReadResponse;
import org.apache.bookkeeper.proto.BookkeeperProtocol.Request;
import org.apache.bookkeeper.proto.BookkeeperProtocol.Response;
import org.apache.bookkeeper.proto.BookkeeperProtocol.StatusCode;
import org.apache.bookkeeper.proto.BookkeeperProtocol.WriteLacRequest;
import org.apache.bookkeeper.proto.BookkeeperProtocol.WriteLacResponse;
import org.apache.bookkeeper.ssl.SSLUtils;
import org.apache.bookkeeper.ssl.SecurityException;
import org.apache.bookkeeper.ssl.SecurityHandlerFactory;
import org.apache.bookkeeper.ssl.SecurityHandlerFactory.NodeType;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.MathUtils;
import org.apache.bookkeeper.util.OrderedSafeExecutor;
import org.apache.bookkeeper.util.SafeRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import com.google.protobuf.ExtensionRegistry;
import java.security.cert.Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import javax.net.ssl.SSLPeerUnverifiedException;
import org.apache.bookkeeper.auth.BookKeeperPrincipal;
import org.apache.bookkeeper.client.ClientConnectionPeer;

/**
 * This class manages all details of connection to a particular bookie. It also
 * has reconnect logic if a connection to a bookie fails.
 *
 */
@Sharable
public class PerChannelBookieClient extends ChannelInboundHandlerAdapter {

    static final Logger LOG = LoggerFactory.getLogger(PerChannelBookieClient.class);

    // this set contains the bookie error return codes that we do not consider for a bookie to be "faulty"
    private static final Set<Integer> expectedBkOperationErrors = Collections.unmodifiableSet(Sets
            .newHashSet(BKException.Code.BookieHandleNotAvailableException,
                        BKException.Code.NoSuchEntryException,
                        BKException.Code.NoSuchLedgerExistsException,
                        BKException.Code.LedgerFencedException,
                        BKException.Code.LedgerExistException,
                        BKException.Code.DuplicateEntryIdException,
                        BKException.Code.WriteOnReadOnlyBookieException));

    public static final int MAX_FRAME_LENGTH = 2 * 1024 * 1024; // 2M
    public static final AtomicLong txnIdGenerator = new AtomicLong(0);

    final BookieSocketAddress addr;
    final EventLoopGroup eventLoopGroup;
    final OrderedSafeExecutor executor;
    final HashedWheelTimer requestTimer;
    final int addEntryTimeout;
    final int readEntryTimeout;
    final int getBookieInfoTimeout;
    final int startTLSTimeout;

    private final ConcurrentHashMap<CompletionKey, CompletionValue> completionObjects = new ConcurrentHashMap<CompletionKey, CompletionValue>();

    private final StatsLogger statsLogger;
    private final OpStatsLogger readEntryOpLogger;
    private final OpStatsLogger readTimeoutOpLogger;
    private final OpStatsLogger addEntryOpLogger;
    private final OpStatsLogger writeLacOpLogger;
    private final OpStatsLogger readLacOpLogger;
    private final OpStatsLogger addTimeoutOpLogger;
    private final OpStatsLogger writeLacTimeoutOpLogger;
    private final OpStatsLogger readLacTimeoutOpLogger;
    private final OpStatsLogger getBookieInfoTimeoutOpLogger;
    private final OpStatsLogger connectTimer;
    private final OpStatsLogger sendWaitTimer;
    private final Counter exceptionCounter;
    private final OpStatsLogger getBookieInfoOpLogger;
    private final OpStatsLogger startTLSOpLogger;
    private final Counter addEntryOutstanding;
    private final Counter readEntryOutstanding;
    /* collect stats on all Ops that flows through netty pipeline */
    private final OpStatsLogger nettyOpLogger;

    // to remove cost of syscalls on thread stack collection create these exceptions once and reuse. 
    private final static Exception notWritableTooLongEx = 
            new Exception("waited for channel to become writable for too long, failing");
    private final static Exception failingConsequentWriteEx = 
            new Exception("waited for the channel to become writable for too long, " 
                    + "fast failing consequent writes");
    static {
        // clear stack trace to reduce amount of logging in extreme cases
        // this a bit complicates troubleshooting (no exact stack and stack lines)
        // but not too much as exception messages are unique
        notWritableTooLongEx.setStackTrace(new StackTraceElement[0]);
        failingConsequentWriteEx.setStackTrace(new StackTraceElement[0]);
    }

    /**
     * The following member variables do not need to be concurrent, or volatile
     * because they are always updated under a lock
     */
    private volatile Queue<GenericCallback<PerChannelBookieClient>> pendingOps =
            new ArrayDeque<GenericCallback<PerChannelBookieClient>>();
    volatile Channel channel = null;
    private final ClientConnectionPeer connectionPeer;
    private volatile BookKeeperPrincipal authorizedId = BookKeeperPrincipal.ANONYMOUS;

    private volatile boolean fastFailEnabled = false;

    enum ConnectionState {
        DISCONNECTED, CONNECTING, CONNECTED, CLOSED, START_TLS
    }

    volatile ConnectionState state;
    final ReentrantReadWriteLock closeLock = new ReentrantReadWriteLock();
    private final ClientConfiguration conf;
    private final long channelWaitOnWriteMillis;

    private final PerChannelBookieClientPool pcbcPool;
    private final ClientAuthProvider.Factory authProviderFactory;
    private final ExtensionRegistry extRegistry;
    private final SecurityHandlerFactory shFactory;
    private final Object channelMonitor = new Object();

    public PerChannelBookieClient(OrderedSafeExecutor executor, EventLoopGroup eventLoopGroup,
                                  BookieSocketAddress addr) throws SecurityException {
        this(new ClientConfiguration(), executor, eventLoopGroup, addr, null, NullStatsLogger.INSTANCE, null, null,
                null);
    }

    public PerChannelBookieClient(OrderedSafeExecutor executor, EventLoopGroup eventLoopGroup,
                                  BookieSocketAddress addr,
                                  ClientAuthProvider.Factory authProviderFactory,
                                  ExtensionRegistry extRegistry) throws SecurityException {
        this(new ClientConfiguration(), executor, eventLoopGroup, addr, null, NullStatsLogger.INSTANCE,
                authProviderFactory, extRegistry, null);
    }

    public PerChannelBookieClient(ClientConfiguration conf, OrderedSafeExecutor executor,
                                  EventLoopGroup eventLoopGroup, BookieSocketAddress addr,
                                  HashedWheelTimer requestTimer, StatsLogger parentStatsLogger,
                                  ClientAuthProvider.Factory authProviderFactory,
                                  ExtensionRegistry extRegistry,
                                  PerChannelBookieClientPool pcbcPool) throws SecurityException {
       this(conf, executor, eventLoopGroup, addr, null, NullStatsLogger.INSTANCE,
                authProviderFactory, extRegistry, pcbcPool, null);
    }

    public PerChannelBookieClient(ClientConfiguration conf, OrderedSafeExecutor executor,
                                  EventLoopGroup eventLoopGroup, BookieSocketAddress addr,
                                  HashedWheelTimer requestTimer, StatsLogger parentStatsLogger,
                                  ClientAuthProvider.Factory authProviderFactory,
                                  ExtensionRegistry extRegistry,
                                  PerChannelBookieClientPool pcbcPool,
                                  SecurityHandlerFactory shFactory) throws SecurityException {
        this.conf = conf;
        this.channelWaitOnWriteMillis = conf.getChannelWaitOnWriteMillis();
        this.addr = addr;
        this.executor = executor;
        if (LocalBookiesRegistry.isLocalBookie(addr)) {
            this.eventLoopGroup = new DefaultEventLoopGroup();
        } else {
            this.eventLoopGroup = eventLoopGroup;
        }
        this.state = ConnectionState.DISCONNECTED;
        this.requestTimer = requestTimer;
        this.addEntryTimeout = conf.getAddEntryTimeout();
        this.readEntryTimeout = conf.getReadEntryTimeout();
        this.getBookieInfoTimeout = conf.getBookieInfoTimeout();
        this.startTLSTimeout = conf.getStartTLSTimeout();

        this.authProviderFactory = authProviderFactory;
        this.extRegistry = extRegistry;
        this.shFactory = shFactory;
        if (shFactory != null) {
            shFactory.init(NodeType.Client, conf);
        }

        StringBuilder nameBuilder = new StringBuilder();
        nameBuilder.append(addr.getHostName().replace('.', '_').replace('-', '_'))
            .append("_").append(addr.getPort());

        this.statsLogger = parentStatsLogger.scope(BookKeeperClientStats.CHANNEL_SCOPE)
            .scope(generateAddressScope(addr));

        this.pcbcPool = pcbcPool;

        this.connectionPeer = new ClientConnectionPeer() {

            @Override
            public SocketAddress getRemoteAddr() {
                Channel c = channel;
                if (c != null) {
                    return c.remoteAddress();
                } else {
                    return null;
                }
            }

            @Override
            public Collection<Object> getProtocolPrincipals() {
                Channel c = channel;
                if (c == null) {
                    return Collections.emptyList();
                }
                SslHandler ssl = c.pipeline().get(SslHandler.class);
                if (ssl == null) {
                    return Collections.emptyList();
                }
                try {
                    Certificate[] certificates = ssl.engine().getSession().getPeerCertificates();
                    if (certificates == null) {
                        return Collections.emptyList();
                    }
                    List<Object> result = new ArrayList<>();
                    result.addAll(Arrays.asList(certificates));
                    return result;
                } catch (SSLPeerUnverifiedException err) {
                     return Collections.emptyList();
                }
            }

            @Override
            public void disconnect() {
                Channel c = channel;
                if (c != null) {
                    c.close()
                        .addListener(x -> channelChanged());
                }
                LOG.info("authplugin disconnected channel {}", channel);
            }

            @Override
            public void setAuthorizedId(BookKeeperPrincipal principal) {
                authorizedId = principal;
                LOG.info("connection {} authenticated as {}", channel, principal);
            }

            @Override
            public BookKeeperPrincipal getAuthorizedId() {
                return authorizedId;
            }

            @Override
            public boolean isSecure() {
               Channel c = channel;
               if (c == null) {
                    return false;
               } else {
                    return c.pipeline().get(SslHandler.class) != null;
               }
            }

        };

        readEntryOpLogger = statsLogger.getOpStatsLogger(BookKeeperClientStats.READ_OP);
        addEntryOpLogger = statsLogger.getOpStatsLogger(BookKeeperClientStats.ADD_OP);
        writeLacOpLogger = statsLogger.getOpStatsLogger(BookKeeperClientStats.WRITE_LAC_OP);
        readLacOpLogger = statsLogger.getOpStatsLogger(BookKeeperClientStats.READ_LAC_OP);
        getBookieInfoOpLogger = statsLogger.getOpStatsLogger(BookKeeperClientStats.GET_BOOKIE_INFO_OP);
        readTimeoutOpLogger = statsLogger.getOpStatsLogger(BookKeeperClientStats.TIMEOUT_READ);
        addTimeoutOpLogger = statsLogger.getOpStatsLogger(BookKeeperClientStats.TIMEOUT_ADD);
        writeLacTimeoutOpLogger = statsLogger.getOpStatsLogger(BookKeeperClientStats.TIMEOUT_WRITE_LAC);
        readLacTimeoutOpLogger = statsLogger.getOpStatsLogger(BookKeeperClientStats.TIMEOUT_READ_LAC);
        getBookieInfoTimeoutOpLogger = statsLogger.getOpStatsLogger(BookKeeperClientStats.TIMEOUT_GET_BOOKIE_INFO);
        exceptionCounter = statsLogger.getCounter(BookKeeperClientStats.NETTY_EXCEPTION_CNT);
        connectTimer = statsLogger.getOpStatsLogger(BookKeeperClientStats.CLIENT_CONNECT_TIMER);
        sendWaitTimer = statsLogger.getOpStatsLogger(BookKeeperClientStats.CLIENT_SEND_WAIT_TIMER);
        startTLSOpLogger = statsLogger.getOpStatsLogger(BookKeeperClientStats.CHANNEL_START_TLS_OP);
        addEntryOutstanding = statsLogger.getCounter(BookKeeperClientStats.ADD_OP_OUTSTANDING);
        readEntryOutstanding = statsLogger.getCounter(BookKeeperClientStats.READ_OP_OUTSTANDING);
        nettyOpLogger = statsLogger.getOpStatsLogger(BookKeeperClientStats.NETTY_OPS);
    }

    private String generateAddressScope(BookieSocketAddress addr) {
        StringBuilder nameBuilder = new StringBuilder();
        return nameBuilder.append(addr.getFullyQualifiedDomainName().replace('.', '_').replace('-', '_')).append("_")
                .append(addr.getPort()).toString();
    }

    private void completeOperation(GenericCallback<PerChannelBookieClient> op, int rc) {
        //Thread.dumpStack();
        closeLock.readLock().lock();
        try {
            if (ConnectionState.CLOSED == state) {
                op.operationComplete(BKException.Code.ClientClosedException, this);
            } else {
                op.operationComplete(rc, this);
            }
        } finally {
            closeLock.readLock().unlock();
        }
    }

    protected ChannelFuture connect() {
        final long startTime = MathUtils.nowInNano();
        LOG.debug("Connecting to bookie: {}", addr);

        // Set up the ClientBootStrap so we can create a new Channel connection to the bookie.
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(eventLoopGroup);
        if (eventLoopGroup instanceof EpollEventLoopGroup) {
            bootstrap.channel(EpollSocketChannel.class);
        } else if (eventLoopGroup instanceof DefaultEventLoopGroup) {
            bootstrap.channel(LocalChannel.class);
        } else {
            bootstrap.channel(NioSocketChannel.class);
        }

        bootstrap.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
        bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, conf.getClientConnectTimeoutMillis());
        bootstrap.option(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(
                conf.getClientWriteBufferLowWaterMark(), conf.getClientWriteBufferHighWaterMark()));

        if (!(eventLoopGroup instanceof DefaultEventLoopGroup)) {
            bootstrap.option(ChannelOption.TCP_NODELAY, conf.getClientTcpNoDelay());
            bootstrap.option(ChannelOption.SO_KEEPALIVE, conf.getClientSockKeepalive());

            // if buffer sizes are 0, let OS auto-tune it
            if (conf.getClientSendBufferSize() > 0) {
                bootstrap.option(ChannelOption.SO_SNDBUF, conf.getClientSendBufferSize());
            }

            if (conf.getClientReceiveBufferSize() > 0) {
                bootstrap.option(ChannelOption.SO_RCVBUF, conf.getClientReceiveBufferSize());
            }
        }

        // In the netty pipeline, we need to split packets based on length, so we
        // use the {@link LengthFieldBasedFramDecoder}. Other than that all actions
        // are carried out in this class, e.g., making sense of received messages,
        // prepending the length to outgoing packets etc.
        bootstrap.handler(new ChannelInitializer<Channel>() {
            @Override
            protected void initChannel(Channel ch) throws Exception {
                ChannelPipeline pipeline = ch.pipeline();

                pipeline.addLast("lengthbasedframedecoder",
                        new LengthFieldBasedFrameDecoder(MAX_FRAME_LENGTH, 0, 4, 0, 4));
                pipeline.addLast("lengthprepender", new LengthFieldPrepender(4));
                pipeline.addLast("bookieProtoEncoder", new BookieProtoEncoding.RequestEncoder(extRegistry));
                pipeline.addLast("bookieProtoDecoder", new BookieProtoEncoding.ResponseDecoder(extRegistry));
                pipeline.addLast("authHandler",
                        new AuthHandler.ClientSideHandler(authProviderFactory, txnIdGenerator, connectionPeer));
                pipeline.addLast("mainhandler", PerChannelBookieClient.this);
            }
        });

        SocketAddress bookieAddr = addr.getSocketAddress();
        if (eventLoopGroup instanceof DefaultEventLoopGroup) {
            bookieAddr = addr.getLocalAddress();
        }

        ChannelFuture future = bootstrap.connect(bookieAddr);
        future.addListener(new ConnectionFutureListener(startTime));
        future.addListener(x -> channelChanged());

        return future;
    }

    void cleanDisconnectAndClose() {
        disconnect();
        close();
    }

    void connectIfNeededAndDoOp(GenericCallback<PerChannelBookieClient> op) {
        boolean completeOpNow = false;
        int opRc = BKException.Code.OK;
        // common case without lock first
        if (channel != null && state == ConnectionState.CONNECTED) {
            completeOpNow = true;
        } else {

            synchronized (this) {
                // check the channel status again under lock
                if (channel != null && state == ConnectionState.CONNECTED) {
                    completeOpNow = true;
                    opRc = BKException.Code.OK;
                } else if (state == ConnectionState.CLOSED) {
                    completeOpNow = true;
                    opRc = BKException.Code.BookieHandleNotAvailableException;
                } else {
                    // channel is either null (first connection attempt), or the
                    // channel is disconnected. Connection attempt is still in
                    // progress, queue up this op. Op will be executed when
                    // connection attempt either fails or succeeds
                    pendingOps.add(op);

                    if (state == ConnectionState.CONNECTING
                        || state == ConnectionState.START_TLS) {
                        // the connection request has already been sent and it is waiting for the response.
                        return;
                    }
                    // switch state to connecting and do connection attempt
                    state = ConnectionState.CONNECTING;
                }
            }
            if (!completeOpNow) {
                // Start connection attempt to the input server host.
                connect();
            }
        }

        if (completeOpNow) {
            completeOperation(op, opRc);
        }
    }

    void writeLac(final long ledgerId, final byte[] masterKey, final long lac, ByteBuf toSend, WriteLacCallback cb,
            Object ctx) {
        final long txnId = getTxnId();
        final CompletionKey completionKey = new CompletionKey(txnId, OperationType.WRITE_LAC);
        // writeLac is mostly like addEntry hence uses addEntryTimeout
        completionObjects.put(completionKey,
                new WriteLacCompletion(writeLacOpLogger, cb, ctx, lac, scheduleTimeout(completionKey, addEntryTimeout)));

        // Build the request
        BKPacketHeader.Builder headerBuilder = BKPacketHeader.newBuilder()
                .setVersion(ProtocolVersion.VERSION_THREE)
                .setOperation(OperationType.WRITE_LAC)
                .setTxnId(txnId);
        WriteLacRequest.Builder writeLacBuilder = WriteLacRequest.newBuilder()
                .setLedgerId(ledgerId)
                .setLac(lac)
                .setMasterKey(ByteString.copyFrom(masterKey))
                .setBody(ByteString.copyFrom(toSend.nioBuffer()));

        final Request writeLacRequest = Request.newBuilder()
                .setHeader(headerBuilder)
                .setWriteLacRequest(writeLacBuilder)
                .build();

        final Channel c = channel;
        if (c == null) {
            errorOutWriteLacKey(completionKey);
            return;
        }
        try {
            final long startTime = MathUtils.nowInNano();
            ChannelFuture future = channelWrite(writeLacRequest);
            future.addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    if (future.isSuccess()) {
                        nettyOpLogger.registerSuccessfulEvent(MathUtils.elapsedNanos(startTime), TimeUnit.NANOSECONDS);
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Successfully wrote request for writeLac LedgerId: {} bookie: {}",
                                    ledgerId, c.remoteAddress());
                        }
                    } else {
                        nettyOpLogger.registerFailedEvent(MathUtils.elapsedNanos(startTime), TimeUnit.NANOSECONDS);
                        if (!(future.cause() instanceof ClosedChannelException)) {
                            LOG.warn("Writing Lac(lid={} to channel {} failed : ",
                                    new Object[] { ledgerId, c, future.cause() });
                        }
                        errorOutWriteLacKey(completionKey);
                    }
                }
            });
        } catch (Throwable e) {
            LOG.warn("writeLac operation failed", e);
            errorOutWriteLacKey(completionKey);
        }
    }

    /**
     * This method should be called only after connection has been checked for
     * {@link #connectIfNeededAndDoOp(GenericCallback)}
     *
     * @param ledgerId
     *          Ledger Id
     * @param masterKey
     *          Master Key
     * @param entryId
     *          Entry Id
     * @param toSend
     *          Buffer to send
     * @param cb
     *          Write callback
     * @param ctx
     *          Write callback context
     * @param options
     *          Add options
     */
    void addEntry(final long ledgerId, byte[] masterKey, final long entryId, ByteBuf toSend, WriteCallback cb,
                  Object ctx, final int options) {
        final long txnId = getTxnId();
        final int entrySize = toSend.readableBytes();
        final CompletionKey completionKey = new CompletionKey(txnId, OperationType.ADD_ENTRY);
        completionObjects.put(completionKey,
                new AddCompletion(this, addEntryOpLogger, cb, ctx, ledgerId, entryId,
                                  scheduleTimeout(completionKey, addEntryTimeout)));

        // Build the request and calculate the total size to be included in the packet.
        BKPacketHeader.Builder headerBuilder = BKPacketHeader.newBuilder()
                .setVersion(ProtocolVersion.VERSION_THREE)
                .setOperation(OperationType.ADD_ENTRY)
                .setTxnId(txnId);

        byte[] toSendArray = new byte[toSend.readableBytes()];
        toSend.getBytes(toSend.readerIndex(), toSendArray);
        AddRequest.Builder addBuilder = AddRequest.newBuilder()
                .setLedgerId(ledgerId)
                .setEntryId(entryId)
                .setMasterKey(ByteString.copyFrom(masterKey))
                .setBody(ByteString.copyFrom(toSendArray));

        if (((short)options & BookieProtocol.FLAG_RECOVERY_ADD) == BookieProtocol.FLAG_RECOVERY_ADD) {
            addBuilder.setFlag(AddRequest.Flag.RECOVERY_ADD);
        }

        final Request addRequest = Request.newBuilder()
                .setHeader(headerBuilder)
                .setAddRequest(addBuilder)
                .build();

        final Channel c = channel;
        if (c == null) {
            errorOutAddKey(completionKey);
            return;
        }
        try {
            final long startTime = MathUtils.nowInNano();
            ChannelFuture future = channelWrite(addRequest, channelWaitOnWriteMillis);
            addEntryOutstanding.inc();
            future.addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    if (future.isSuccess()) {
                        nettyOpLogger.registerSuccessfulEvent(MathUtils.elapsedNanos(startTime), TimeUnit.NANOSECONDS);
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Successfully wrote request for adding entry: " + entryId + " ledger-id: " + ledgerId
                                                            + " bookie: " + c.remoteAddress() + " entry length: " + entrySize);
                        }
                        // totalBytesOutstanding.addAndGet(entrySize);
                    } else {
                        nettyOpLogger.registerFailedEvent(MathUtils.elapsedNanos(startTime), TimeUnit.NANOSECONDS);
                        if (!(future.cause() instanceof ClosedChannelException)) {
                            LOG.warn("Writing addEntry(lid={}, eid={}) to channel {} failed : ",
                                    new Object[] { ledgerId, entryId, c, future.cause() });
                        }
                        errorOutAddKey(completionKey);
                    }
                }
            });
        } catch (Throwable e) {
            LOG.warn("Add entry operation failed", e);
            errorOutAddKey(completionKey);
        }
    }

    public void readEntryAndFenceLedger(final long ledgerId, byte[] masterKey,
                                        final long entryId,
                                        ReadEntryCallback cb, Object ctx) {
        final long txnId = getTxnId();
        final CompletionKey completionKey = new CompletionKey(txnId, OperationType.READ_ENTRY);
        completionObjects.put(completionKey,
                new ReadCompletion(this, readEntryOpLogger, cb, ctx, ledgerId, entryId,
                                   scheduleTimeout(completionKey, readEntryTimeout)));

        // Build the request and calculate the total size to be included in the packet.
        BKPacketHeader.Builder headerBuilder = BKPacketHeader.newBuilder()
                .setVersion(ProtocolVersion.VERSION_THREE)
                .setOperation(OperationType.READ_ENTRY)
                .setTxnId(txnId);

        ReadRequest.Builder readBuilder = ReadRequest.newBuilder()
                .setLedgerId(ledgerId)
                .setEntryId(entryId)
                .setMasterKey(ByteString.copyFrom(masterKey))
                .setFlag(ReadRequest.Flag.FENCE_LEDGER);

        final Request readRequest = Request.newBuilder()
                .setHeader(headerBuilder)
                .setReadRequest(readBuilder)
                .build();

        final Channel c = channel;
        if (c == null) {
            errorOutReadKey(completionKey);
            return;
        }

        try {
            final long startTime = MathUtils.nowInNano();
            ChannelFuture future = channelWrite(readRequest);
            future.addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture future) throws Exception {
                        if (future.isSuccess()) {
                        nettyOpLogger.registerSuccessfulEvent(MathUtils.elapsedNanos(startTime), TimeUnit.NANOSECONDS);
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("Successfully wrote request {} to {}",
                                          readRequest, c.remoteAddress());
                            }
                        } else {
                        nettyOpLogger.registerFailedEvent(MathUtils.elapsedNanos(startTime), TimeUnit.NANOSECONDS);
                            if (!(future.cause() instanceof ClosedChannelException)) {
                                LOG.warn("Writing readEntryAndFenceLedger(lid={}, eid={}) to channel {} failed : ",
                                        new Object[] { ledgerId, entryId, c, future.cause() });
                            }
                            errorOutReadKey(completionKey);
                        }
                    }
                });
        } catch(Throwable e) {
            LOG.warn("Read entry operation {} failed", completionKey, e);
            errorOutReadKey(completionKey);
        }
    }

    public void readLac(final long ledgerId, ReadLacCallback cb, Object ctx) {
        final long txnId = getTxnId();
        final CompletionKey completionKey = new CompletionKey(txnId, OperationType.READ_LAC);
        completionObjects.put(completionKey,
                new ReadLacCompletion(readLacOpLogger, cb, ctx, ledgerId,
                        scheduleTimeout(completionKey, readEntryTimeout)));
        // Build the request and calculate the total size to be included in the packet.
        BKPacketHeader.Builder headerBuilder = BKPacketHeader.newBuilder()
                .setVersion(ProtocolVersion.VERSION_THREE)
                .setOperation(OperationType.READ_LAC)
                .setTxnId(txnId);
        ReadLacRequest.Builder readLacBuilder = ReadLacRequest.newBuilder()
                .setLedgerId(ledgerId);
        final Request readLacRequest = Request.newBuilder()
                .setHeader(headerBuilder)
                .setReadLacRequest(readLacBuilder)
                .build();
        final Channel c = channel;
        if (c == null) {
            errorOutReadLacKey(completionKey);
            return;
        }

        try {
            final long startTime = MathUtils.nowInNano();
            ChannelFuture future = channelWrite(readLacRequest);
            future.addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    if (future.isSuccess()) {
                        nettyOpLogger.registerSuccessfulEvent(MathUtils.elapsedNanos(startTime), TimeUnit.NANOSECONDS);
                        LOG.debug("Succssfully wrote request {} to {}",
                                readLacRequest, c.remoteAddress());
                    } else {
                        nettyOpLogger.registerFailedEvent(MathUtils.elapsedNanos(startTime), TimeUnit.NANOSECONDS);
                        if (!(future.cause() instanceof ClosedChannelException)) {
                            LOG.warn("Writing readLac(lid = {}) to channel {} failed : ",
                                    new Object[] { ledgerId, c, future.cause() });
                        }
                        errorOutReadLacKey(completionKey);
                    }
                }
            });
        } catch(Throwable e) {
            LOG.warn("Read LAC operation {} failed", readLacRequest, e);
            errorOutReadLacKey(completionKey);
        }
    }

    public void readEntry(final long ledgerId, final long entryId, ReadEntryCallback cb, Object ctx) {
        final long txnId = getTxnId();
        final CompletionKey completionKey = new CompletionKey(txnId, OperationType.READ_ENTRY);
        completionObjects.put(completionKey,
                new ReadCompletion(this, readEntryOpLogger, cb, ctx, ledgerId, entryId,
                                   scheduleTimeout(completionKey, readEntryTimeout)));

        // Build the request and calculate the total size to be included in the packet.
        BKPacketHeader.Builder headerBuilder = BKPacketHeader.newBuilder()
                .setVersion(ProtocolVersion.VERSION_THREE)
                .setOperation(OperationType.READ_ENTRY)
                .setTxnId(txnId);

        ReadRequest.Builder readBuilder = ReadRequest.newBuilder()
                .setLedgerId(ledgerId)
                .setEntryId(entryId);

        final Request readRequest = Request.newBuilder()
                .setHeader(headerBuilder)
                .setReadRequest(readBuilder)
                .build();

        final Channel c = channel;
        if (c == null) {
            errorOutReadKey(completionKey);
            return;
        }

        try{
            final long startTime = MathUtils.nowInNano();
            ChannelFuture future = channelWrite(readRequest);
            readEntryOutstanding.inc();
            future.addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    if (future.isSuccess()) {
                        nettyOpLogger.registerSuccessfulEvent(MathUtils.elapsedNanos(startTime), TimeUnit.NANOSECONDS);
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Successfully wrote request {} to {}",
                                      readRequest, c.remoteAddress());
                        }
                    } else {
                        nettyOpLogger.registerFailedEvent(MathUtils.elapsedNanos(startTime), TimeUnit.NANOSECONDS);
                        if (!(future.cause() instanceof ClosedChannelException)) {
                            LOG.warn("Writing readEntry(lid={}, eid={}) to channel {} failed : ",
                                    new Object[] { ledgerId, entryId, c, future.cause() });
                        }
                        errorOutReadKey(completionKey);
                    }
                }
            });
        } catch(Throwable e) {
            LOG.warn("Read entry operation {} failed", readRequest, e);
            errorOutReadKey(completionKey);
        }
    }

    public void getBookieInfo(final long requested, GetBookieInfoCallback cb, Object ctx) {
        final long txnId = getTxnId();
        final CompletionKey completionKey = new CompletionKey(txnId, OperationType.GET_BOOKIE_INFO);
        completionObjects.put(completionKey,
                new GetBookieInfoCompletion(this, getBookieInfoOpLogger, cb, ctx,
                                   scheduleTimeout(completionKey, getBookieInfoTimeout)));

        // Build the request and calculate the total size to be included in the packet.
        BKPacketHeader.Builder headerBuilder = BKPacketHeader.newBuilder()
                .setVersion(ProtocolVersion.VERSION_THREE)
                .setOperation(OperationType.GET_BOOKIE_INFO)
                .setTxnId(txnId);

        GetBookieInfoRequest.Builder getBookieInfoBuilder = GetBookieInfoRequest.newBuilder()
                .setRequested(requested);

        final Request getBookieInfoRequest = Request.newBuilder()
                .setHeader(headerBuilder)
                .setGetBookieInfoRequest(getBookieInfoBuilder)
                .build();

        final Channel c = channel;
        if (c == null) {
            errorOutGetBookieInfoKey(completionKey);
            return;
        }

        try{
            final long startTime = MathUtils.nowInNano();
            ChannelFuture future = channelWrite(getBookieInfoRequest);
            future.addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    if (future.isSuccess()) {
                        nettyOpLogger.registerSuccessfulEvent(MathUtils.elapsedNanos(startTime), TimeUnit.NANOSECONDS);
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Successfully wrote request {} to {}",
                                    getBookieInfoRequest, c.remoteAddress());
                        }
                    } else {
                        nettyOpLogger.registerFailedEvent(MathUtils.elapsedNanos(startTime), TimeUnit.NANOSECONDS);
                        if (!(future.cause() instanceof ClosedChannelException)) {
                            LOG.warn("Writing GetBookieInfoRequest(flags={}) to channel {} failed : ",
                                    new Object[] { requested, c, future.cause() });
                        }
                        errorOutGetBookieInfoKey(completionKey);
                    }
                }
            });
        } catch(Throwable e) {
            LOG.warn("Get metadata operation {} failed", getBookieInfoRequest, e);
            errorOutGetBookieInfoKey(completionKey);
        }
    }

    /**
     * Disconnects the bookie client. It can be reused.
     */
    public void disconnect() {
        disconnect(true);
    }

    public void disconnect(boolean wait) {
        LOG.info("Disconnecting the per channel bookie client for {}", addr);
        closeInternal(false, wait);
    }

    /**
     * Closes the bookie client permanently. It cannot be reused.
     * But, does not wait for completion of channel close.
     */
    public void close() {
        close(false);
    }

    /**
     * Closes the bookie client permanently. It cannot be reused.
     *
     * @param wait will wait for channel to close if set to true.
     *             when true, caller will be blocked indefinitely until
     *             channel is closed. So, this method should be called
     *             outside of netty IO thread context to avoid deadlock.
     */
    public void close(boolean wait) {
        LOG.info("Closing the per channel bookie client for {}", addr);
        closeLock.writeLock().lock();
        try {
            if (ConnectionState.CLOSED == state) {
                return;
            }
            state = ConnectionState.CLOSED;
            errorOutOutstandingEntries(BKException.Code.ClientClosedException);
        } finally {
            closeLock.writeLock().unlock();
        }
        closeInternal(true, wait);
    }

    private void closeInternal(boolean permanent, boolean wait) {
        Channel toClose;
        synchronized (this) {
            if (permanent) {
                state = ConnectionState.CLOSED;
            } else if (state != ConnectionState.CLOSED) {
                state = ConnectionState.DISCONNECTED;
            }
            toClose = channel;
            channel = null;
        }

        LOG.info("Closing channel: {} with wait: {}, pcbc new state: {}", toClose, wait, state);
        if (toClose != null) {
            ChannelFuture cf = closeChannel(toClose);
            if (wait) {
                cf.awaitUninterruptibly();
            }
        }
    }

    private ChannelFuture closeChannel(Channel c) {
        LOG.debug("Closing channel {}", c);
        final ChannelFuture cf = c.close();
        cf.addListener(x -> channelChanged());
        return cf;
    }

    void errorStartTLS(int rc) {
        failSSL(rc);
    }

    void errorOutReadKey(final CompletionKey key) {
        errorOutReadKey(key, BKException.Code.BookieHandleNotAvailableException);
    }

    void errorOutReadKey(final CompletionKey key, final int rc) {
        final ReadCompletion readCompletion = (ReadCompletion)completionObjects.remove(key);
        if (null == readCompletion) {
            return;
        }
        executor.submitOrdered(readCompletion.ledgerId, new SafeRunnable() {
            @Override
            public void safeRun() {
                String bAddress = "null";
                Channel c = channel;
                if (c != null && c.remoteAddress() != null) {
                    bAddress = c.remoteAddress().toString();
                }

                LOG.debug("Could not write request for reading entry: {} ledger-id: {} bookie: {} rc: {}",
                        new Object[]{ readCompletion.entryId, readCompletion.ledgerId, bAddress, rc });

                readCompletion.cb.readEntryComplete(rc, readCompletion.ledgerId, readCompletion.entryId,
                                                    null, readCompletion.ctx);
            }

            @Override
            public String toString() {
                return String.format("ErrorOutReadKey(%s)", key);
            }
        });
    }

    void errorOutWriteLacKey(final CompletionKey key) {
        errorOutWriteLacKey(key, BKException.Code.BookieHandleNotAvailableException);
    }

    void errorOutWriteLacKey(final CompletionKey key, final int rc) {
        final WriteLacCompletion writeLacCompletion = (WriteLacCompletion)completionObjects.remove(key);
        if (null == writeLacCompletion) {
            return;
        }
        executor.submitOrdered(writeLacCompletion.ledgerId, new SafeRunnable() {
            @Override
            public void safeRun() {
                String bAddress = "null";
                Channel c = channel;
                if (c != null) {
                    bAddress = c.remoteAddress().toString();
                }
                LOG.debug("Could not write request writeLac for ledgerId: {} bookie: {}",
                          new Object[] { writeLacCompletion.ledgerId, bAddress});
                writeLacCompletion.cb.writeLacComplete(rc, writeLacCompletion.ledgerId, addr, writeLacCompletion.ctx);
            }
        });
    }

    void errorOutReadLacKey(final CompletionKey key) {
        errorOutReadLacKey(key, BKException.Code.BookieHandleNotAvailableException);
    }

    void errorOutReadLacKey(final CompletionKey key, final int rc) {
        final ReadLacCompletion readLacCompletion = (ReadLacCompletion)completionObjects.remove(key);
        if (null == readLacCompletion) {
            return;
        }
        executor.submitOrdered(readLacCompletion.ledgerId, new SafeRunnable() {
            @Override
            public void safeRun() {
                String bAddress = "null";
                Channel c = channel;
                if (c != null) {
                    bAddress = c.remoteAddress().toString();
                }
                LOG.debug("Could not write request readLac for ledgerId: {} bookie: {}",
                          new Object[] { readLacCompletion.ledgerId, bAddress});
                readLacCompletion.cb.readLacComplete(rc, readLacCompletion.ledgerId, null, null, readLacCompletion.ctx);
            }
        });
    }

    void errorOutAddKey(final CompletionKey key) {
        errorOutAddKey(key, BKException.Code.BookieHandleNotAvailableException);
    }

    void errorOutAddKey(final CompletionKey key, final int rc) {
        final AddCompletion addCompletion = (AddCompletion)completionObjects.remove(key);
        if (null == addCompletion) {
            return;
        }
        executor.submitOrdered(addCompletion.ledgerId, new SafeRunnable() {
            @Override
            public void safeRun() {
                String bAddress = "null";
                Channel c = channel;
                if (c != null && c.remoteAddress() != null) {
                    bAddress = c.remoteAddress().toString();
                }
                LOG.debug("Could not write request for adding entry: {} ledger-id: {} bookie: {} rc: {}",
                          new Object[] { addCompletion.entryId, addCompletion.ledgerId, bAddress, rc });

                addCompletion.cb.writeComplete(rc, addCompletion.ledgerId, addCompletion.entryId,
                                               addr, addCompletion.ctx);
                LOG.debug("Invoked callback method: {}", addCompletion.entryId);
            }

            @Override
            public String toString() {
                return String.format("ErrorOutAddKey(%s)", key);
            }
        });
    }

    void errorOutGetBookieInfoKey(final CompletionKey key) {
        errorOutGetBookieInfoKey(key, BKException.Code.BookieHandleNotAvailableException);
    }

    void errorOutGetBookieInfoKey(final CompletionKey key, final int rc) {
        final GetBookieInfoCompletion getBookieInfoCompletion = (GetBookieInfoCompletion)completionObjects.remove(key);
        if (null == getBookieInfoCompletion) {
            return;
        }
        executor.submit(new SafeRunnable() {
            @Override
            public void safeRun() {
                String bAddress = "null";
                Channel c = channel;
                if (c != null) {
                    bAddress = c.remoteAddress().toString();
                }
                LOG.debug("Could not write getBookieInfo request for bookie: {}", new Object[] {bAddress});
                getBookieInfoCompletion.cb.getBookieInfoComplete(rc, new BookieInfo(), getBookieInfoCompletion.ctx);
            }
        });
    }

    /**
     * Errors out pending ops from per channel bookie client. As the channel
     * is being closed, all the operations waiting on the connection
     * will be sent to completion with error
     */
    void errorOutPendingOps(int rc) {
        Queue<GenericCallback<PerChannelBookieClient>> oldPendingOps;
        synchronized (this) {
            oldPendingOps = pendingOps;
            pendingOps = new ArrayDeque<>();
        }

        for (GenericCallback<PerChannelBookieClient> pendingOp : oldPendingOps) {
            pendingOp.operationComplete(rc, PerChannelBookieClient.this);
        }
    }

    /**
     * Errors out pending entries. We call this method from one thread to avoid
     * concurrent executions to QuorumOpMonitor (implements callbacks). It seems
     * simpler to call it from BookieHandle instead of calling directly from
     * here.
     */

    void errorOutOutstandingEntries(int rc) {

        // DO NOT rewrite these using Map.Entry iterations. We want to iterate
        // on keys and see if we are successfully able to remove the key from
        // the map. Because the add and the read methods also do the same thing
        // in case they get a write failure on the socket. The one who
        // successfully removes the key from the map is the one responsible for
        // calling the application callback.
        Enumeration<CompletionKey> keys = completionObjects.keys();
        while (keys.hasMoreElements()) {
            CompletionKey key = keys.nextElement();
            switch (key.operationType) {
                case ADD_ENTRY:
                    errorOutAddKey(key, rc);
                    break;
                case READ_ENTRY:
                    errorOutReadKey(key, rc);
                    break;
                case GET_BOOKIE_INFO:
                    errorOutGetBookieInfoKey(key, rc);
                    break;
                case START_TLS:
                    errorStartTLS(rc);
                    break;
                default:
                    break;
            }
        }
    }

    void recordError() {
        if (pcbcPool != null) {
            pcbcPool.recordError();
        }
    }

    /**
     * If our channel has disconnected, we just error out the pending entries
     */
    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        LOG.info("Disconnected from bookie channel {}", ctx.channel());
        if (ctx.channel() != null) {
            closeChannel(ctx.channel());
        }

        errorOutOutstandingEntries(BKException.Code.BookieHandleNotAvailableException);
        errorOutPendingOps(BKException.Code.BookieHandleNotAvailableException);

        synchronized (this) {
            if (this.channel == ctx.channel()
                && state != ConnectionState.CLOSED) {
                state = ConnectionState.DISCONNECTED;
            }
        }

        // we don't want to reconnect right away. If someone sends a request to
        // this address, we will reconnect.
    }

    /**
     * Called by netty when an exception happens in one of the netty threads
     * (mostly due to what we do in the netty threads)
     */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        exceptionCounter.inc();
        if (cause instanceof CorruptedFrameException || cause instanceof TooLongFrameException) {
            LOG.error("Corrupted frame received from bookie: {}", ctx.channel().remoteAddress());
            ctx.close();
            return;
        }

        if (cause instanceof AuthHandler.AuthenticationException) {
            LOG.error("Error authenticating connection", cause);
            errorOutOutstandingEntries(BKException.Code.UnauthorizedAccessException);
            Channel c = ctx.channel();
            if (c != null) {
                closeChannel(c);
            }
            return;
        }

        if (cause instanceof IOException) {
            LOG.warn("Exception caught on:{} cause:", ctx.channel(), cause);
            ctx.close();
            return;
        }

        synchronized (this) {
            if (state == ConnectionState.CLOSED) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Unexpected exception caught by bookie client channel handler, "
                            + "but the client is closed, so it isn't important", cause);
                }
            } else {
                LOG.error("Unexpected exception caught by bookie client channel handler", cause);
            }
        }

        // Since we are a library, cant terminate App here, can we?
        ctx.close();
    }

    /**
     * Called by netty when a message is received on a channel
     */
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (!(msg instanceof Response)) {
            ctx.fireChannelRead(msg);
            return;
        }

        final Response response = (Response) msg;
        final BKPacketHeader header = response.getHeader();

        final CompletionValue completionValue = completionObjects.get(newCompletionKey(header.getTxnId(),
                header.getOperation()));

        if (null == completionValue) {
            // Unexpected response, so log it. The txnId should have been present.
            if (LOG.isDebugEnabled()) {
                LOG.debug("Unexpected response received from bookie : " + addr + " for type : "
                        + header.getOperation() + " and txnId : " + header.getTxnId());
            }
        } else {
            long orderingKey = completionValue.ledgerId;
            executor.submitOrdered(orderingKey, new SafeRunnable() {
                @Override
                public void safeRun() {
                    OperationType type = header.getOperation();
                    switch (type) {
                        case ADD_ENTRY:
                            handleAddResponse(response, completionValue);
                            break;
                        case READ_ENTRY:
                            handleReadResponse(response, completionValue);
                            break;
                        case WRITE_LAC:
                            handleWriteLacResponse(response.getWriteLacResponse(), completionValue);
                            break;
                        case READ_LAC:
                            handleReadLacResponse(response.getReadLacResponse(), completionValue);
                            break;
                        case GET_BOOKIE_INFO:
                            handleGetBookieInfoResponse(response, completionValue);
                            break;
                        case START_TLS:
                            StatusCode status = response.getStatus();
                            handleStartTLSResponse(status, completionValue);
                            break;
                        default:
                            LOG.error("Unexpected response, type:{} received from bookie:{}, ignoring",
                                      type, addr);
                            break;
                    }
                }

                @Override
                public String toString() {
                    return String.format("HandleResponse(Txn=%d, Type=%s, Entry=(%d, %d))",
                                         header.getTxnId(), header.getOperation(),
                                         completionValue.ledgerId, completionValue.entryId);
                }
            });
        }

        completionObjects.remove(newCompletionKey(header.getTxnId(), header.getOperation()));
    }

    void handleStartTLSResponse(StatusCode status, CompletionValue completionValue) {
        StartTLSCompletion tlsCompletion = (StartTLSCompletion) completionValue;

        // convert to BKException code because thats what the upper
        // layers expect. This is UGLY, there should just be one set of
        // error codes.
        Integer rcToRet = statusCodeToExceptionCode(status);
        if (null == rcToRet) {
            LOG.error("START_TLS failed on bookie:{}", addr);
            rcToRet = BKException.Code.SecurityException;
        } else {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Received START_TLS response from {} rc: {}", addr, rcToRet);
            }
        }

        // Cancel START_TLS request timeout
        tlsCompletion.cb.startTLSComplete(rcToRet, tlsCompletion.ctx);

        if (state != ConnectionState.START_TLS) {
            LOG.error("Connection state changed before TLS response received");
            failSSL(BKException.Code.BookieHandleNotAvailableException);
        } else if (status != StatusCode.EOK) {
            LOG.error("Client received error {} during TLS negotiation", status);
            failSSL(BKException.Code.SecurityException);
        } else {
            // create SSL handler
            PerChannelBookieClient parentObj = PerChannelBookieClient.this;
            SslHandler handler = parentObj.shFactory.newSslHandler();
            if (handler == null) {
                LOG.error("failed to get SSL handler");
                failSSL(BKException.Code.SecurityException);
                return;
            }

            channel.pipeline().addFirst(parentObj.shFactory.getHandlerName(), handler);
            handler.handshakeFuture().addListener(new GenericFutureListener<Future<Channel>>() {
                @Override
                public void operationComplete(Future<Channel> future) throws Exception {
                    int rc;
                    Queue<GenericCallback<PerChannelBookieClient>> oldPendingOps;

                    synchronized (PerChannelBookieClient.this) {
                        if (future.isSuccess() && state == ConnectionState.CONNECTING) {
                            LOG.error("Connection state changed before TLS handshake completed {}/{}", addr, state);
                            rc = BKException.Code.BookieHandleNotAvailableException;
                            closeChannel(channel);
                            channel = null;
                            if (state != ConnectionState.CLOSED) {
                                state = ConnectionState.DISCONNECTED;
                            }
                        } else if (future.isSuccess() && state == ConnectionState.START_TLS) {
                            rc = BKException.Code.OK;
                            LOG.info("Successfully connected to bookie using SSL: {}", addr);

                            state = ConnectionState.CONNECTED;

                            // print peer credentials
                            SslHandler sslHandler = (SslHandler) channel.pipeline().get(SslHandler.class);
                            LOG.info("Session {} is protected by {} ", future.get(),
                                    sslHandler.engine().getSession().getCipherSuite());

                            Certificate[] certificates = sslHandler.engine().getSession().getPeerCertificates();
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("Peer Certificate Chain for {}: {} ", future.get(),
                                        SSLUtils.prettyPrintCertChain(certificates));
                            }

                            AuthHandler.ClientSideHandler authHandler = future.get().pipeline()
                                    .get(AuthHandler.ClientSideHandler.class);
                            authHandler.authProvider.onProtocolUpgrade();
                        } else if (future.isSuccess()
                                && (state == ConnectionState.CLOSED || state == ConnectionState.DISCONNECTED)) {
                            LOG.warn("Closed before TLS handshake completed, clean up: {}, current state {}",
                                    channel, state);
                            closeChannel(channel);
                            rc = BKException.Code.BookieHandleNotAvailableException;
                            channel = null;
                        } else if (future.isSuccess() && state == ConnectionState.CONNECTED) {
                            LOG.debug("Already connected with another channel({}), so close the new channel({})",
                                    channel, channel);
                            closeChannel(channel);
                            return; // pendingOps should have been completed when other channel connected
                        } else {
                            LOG.error("TLS handshake failed with bookie: {}/{}, current state {} : ",
                                    new Object[] { channel, addr, state, future.cause() });
                            rc = BKException.Code.SecurityException;
                            closeChannel(channel);
                            channel = null;
                            if (state != ConnectionState.CLOSED) {
                                state = ConnectionState.DISCONNECTED;
                            }
                        }

                        // trick to not do operations under the lock, take the list
                        // of pending ops and assign it to a new variable, while
                        // emptying the pending ops by just assigning it to a new
                        // list
                        oldPendingOps = pendingOps;
                        pendingOps = new ArrayDeque<>();
                    }

                    for (GenericCallback<PerChannelBookieClient> pendingOp : oldPendingOps) {
                        pendingOp.operationComplete(rc, PerChannelBookieClient.this);
                    }
                }
            });
        }
    }

    void handleWriteLacResponse(WriteLacResponse writeLacResponse, CompletionValue completionValue) {
        // The completion value should always be an instance of an WriteLacCompletion object when we reach here.
        WriteLacCompletion plc = (WriteLacCompletion)completionValue;

        long ledgerId = writeLacResponse.getLedgerId();
        StatusCode status = writeLacResponse.getStatus();

        LOG.debug("Got response for writeLac request from bookie: " + addr + " for ledger: " + ledgerId + " rc: " + status);

        // convert to BKException code
        Integer rcToRet = statusCodeToExceptionCode(status);
        if (null == rcToRet) {
            LOG.error("writeLac for ledger: " + ledgerId + " failed on bookie: " + addr
                        + " with code:" + status);
            rcToRet = BKException.Code.WriteException;
        }
        plc.cb.writeLacComplete(rcToRet, ledgerId, addr, plc.ctx);
    }

    void handleAddResponse(Response response, CompletionValue completionValue) {
        // The completion value should always be an instance of an AddCompletion object when we reach here.
        AddCompletion ac = (AddCompletion)completionValue;
        AddResponse addResponse = response.getAddResponse();

        long ledgerId = addResponse.getLedgerId();
        long entryId = addResponse.getEntryId();
        StatusCode status = response.getStatus() == StatusCode.EOK ? addResponse.getStatus() : response.getStatus();

        addEntryOutstanding.dec();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Got response for add request from bookie: " + addr + " for ledger: " + ledgerId + " entry: "
                    + entryId + " rc: " + status);
        }
        // convert to BKException code because thats what the upper
        // layers expect. This is UGLY, there should just be one set of
        // error codes.
        Integer rcToRet = statusCodeToExceptionCode(status);
        if (null == rcToRet) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Add for ledger: " + ledgerId + ", entry: " + entryId + " failed on bookie: " + addr
                        + " with code:" + status);
            }
            rcToRet = BKException.Code.WriteException;
        }
        ac.cb.writeComplete(rcToRet, ledgerId, entryId, addr, ac.ctx);
    }

    void handleReadLacResponse(ReadLacResponse readLacResponse, CompletionValue completionValue) {
        // The completion value should always be an instance of an WriteLacCompletion object when we reach here.
        ReadLacCompletion glac = (ReadLacCompletion)completionValue;

        long ledgerId = readLacResponse.getLedgerId();
        StatusCode status = readLacResponse.getStatus();
        ByteBuf lacBuffer = Unpooled.EMPTY_BUFFER;
        ByteBuf lastEntryBuffer = Unpooled.EMPTY_BUFFER;

       // Thread.dumpStack();

        if (readLacResponse.hasLacBody()) {
            lacBuffer = Unpooled.wrappedBuffer(readLacResponse.getLacBody().asReadOnlyByteBuffer());
        }

        if (readLacResponse.hasLastEntryBody()) {
            lastEntryBuffer = Unpooled.wrappedBuffer(readLacResponse.getLastEntryBody().asReadOnlyByteBuffer());
        }

        LOG.debug("Got response for readLac request from bookie: " + addr + " for ledger: " + ledgerId + " rc: " + status);
        // convert to BKException code
        Integer rcToRet = statusCodeToExceptionCode(status);
        if (null == rcToRet) {
            LOG.debug("readLac for ledger: " + ledgerId + " failed on bookie: " + addr
                      + " with code:" + status);
            rcToRet = BKException.Code.ReadException;
        }
        glac.cb.readLacComplete(rcToRet, ledgerId, lacBuffer.slice(), lastEntryBuffer.slice(), glac.ctx);
    }

    void handleReadResponse(Response response, CompletionValue completionValue) {
        // The completion value should always be an instance of a ReadCompletion object when we reach here.
        ReadCompletion rc = (ReadCompletion)completionValue;
        ReadResponse readResponse = response.getReadResponse();

        long ledgerId = readResponse.getLedgerId();
        long entryId = readResponse.getEntryId();
        StatusCode status = response.getStatus() == StatusCode.EOK ? readResponse.getStatus() : response.getStatus();

        ByteBuf buffer = Unpooled.EMPTY_BUFFER;

        if (readResponse.hasBody()) {
            buffer = Unpooled.wrappedBuffer(readResponse.getBody().asReadOnlyByteBuffer());
        }

        readEntryOutstanding.dec();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Got response for read request from bookie: " + addr + " for ledger: " + ledgerId + " entry: "
                    + entryId + " rc: " + rc + " entry length: " + buffer.readableBytes());
        }

        // convert to BKException code because thats what the uppper
        // layers expect. This is UGLY, there should just be one set of
        // error codes.
        Integer rcToRet = statusCodeToExceptionCode(status);
        if (null == rcToRet) {
            LOG.error("Read entry for ledger:{}, entry:{} failed on bookie:{} with code:{}",
                      new Object[] { ledgerId, entryId, addr, status });
            rcToRet = BKException.Code.ReadException;
        }
        rc.cb.readEntryComplete(rcToRet, ledgerId, entryId, buffer.slice(), rc.ctx);
    }

    void handleGetBookieInfoResponse(Response response, CompletionValue completionValue) {
        // The completion value should always be an instance of a GetBookieInfoCompletion object when we reach here.
        GetBookieInfoCompletion rc = (GetBookieInfoCompletion)completionValue;
        GetBookieInfoResponse getBookieInfoResponse = response.getGetBookieInfoResponse();

        long freeDiskSpace = getBookieInfoResponse.hasFreeDiskSpace() ? getBookieInfoResponse.getFreeDiskSpace() : 0L;
        long totalDiskCapacity = getBookieInfoResponse.hasTotalDiskCapacity() ? getBookieInfoResponse.getTotalDiskCapacity() : 0L;

        StatusCode status = response.getStatus() == StatusCode.EOK ? getBookieInfoResponse.getStatus() : response.getStatus();

        if (LOG.isDebugEnabled()) {
            LOG.debug("Got response for read metadata request from bookie: {} rc {}", addr, rc);
        }

        // convert to BKException code because thats what the upper
        // layers expect. This is UGLY, there should just be one set of
        // error codes.
        Integer rcToRet = statusCodeToExceptionCode(status);
        if (null == rcToRet) {
            LOG.error("Read metadata failed on bookie:{} with code:{}",
                      new Object[] { addr, status });
            rcToRet = BKException.Code.ReadException;
        }
        LOG.debug("Response received from bookie info read: freeDiskSpace=" +  freeDiskSpace + " totalDiskSpace:" + totalDiskCapacity);
        rc.cb.getBookieInfoComplete(rcToRet, new BookieInfo(totalDiskCapacity, freeDiskSpace), rc.ctx);
    }

    /**
     * Boiler-plate wrapper classes follow
     *
     */

    // visible for testing
    static abstract class CompletionValue {
        final Object ctx;
        protected final long ledgerId;
        protected final long entryId;
        protected final Timeout timeout;

        public CompletionValue(Object ctx, long ledgerId, long entryId,
                               Timeout timeout) {
            this.ctx = ctx;
            this.ledgerId = ledgerId;
            this.entryId = entryId;
            this.timeout = timeout;
        }

        void cancelTimeout() {
            if (null != timeout) {
                timeout.cancel();
            }
        }
    }

    // visible for testing
    static class WriteLacCompletion extends CompletionValue {
        final WriteLacCallback cb;

        public WriteLacCompletion(WriteLacCallback cb, Object ctx, long ledgerId) {
            this(null, cb, ctx, ledgerId, null);
        }

        public WriteLacCompletion(final OpStatsLogger writeLacOpLogger, final WriteLacCallback originalCallback,
                final Object originalCtx, final long ledgerId, final Timeout timeout) {
            super(originalCtx, ledgerId, BookieProtocol.LAST_ADD_CONFIRMED, timeout);
            final long startTime = MathUtils.nowInNano();
            this.cb = null == writeLacOpLogger ? originalCallback : new WriteLacCallback() {
                @Override
                public void writeLacComplete(int rc, long ledgerId, BookieSocketAddress addr, Object ctx) {
                    cancelTimeout();
                    long latency = MathUtils.elapsedNanos(startTime);
                    if (rc != BKException.Code.OK) {
                        writeLacOpLogger.registerFailedEvent(latency, TimeUnit.NANOSECONDS);
                    } else {
                        writeLacOpLogger.registerSuccessfulEvent(latency, TimeUnit.NANOSECONDS);
                    }
                    originalCallback.writeLacComplete(rc, ledgerId, addr, originalCtx);
                }
            };

        }
    }

    // visible for testing
    static class ReadLacCompletion extends CompletionValue {
        final ReadLacCallback cb;

        public ReadLacCompletion(ReadLacCallback cb, Object ctx, long ledgerId) {
            this (null, cb, ctx, ledgerId, null);
        }

        public ReadLacCompletion(final OpStatsLogger readLacOpLogger, final ReadLacCallback originalCallback,
                final Object ctx, final long ledgerId, final Timeout timeout) {
            super(ctx, ledgerId, BookieProtocol.LAST_ADD_CONFIRMED, timeout);
            final long startTime = MathUtils.nowInNano();
            this.cb = null == readLacOpLogger ? originalCallback : new ReadLacCallback() {
                @Override
                public void readLacComplete(int rc, long ledgerId, ByteBuf lacBuffer, ByteBuf lastEntryBuffer,
                        Object ctx) {
                    cancelTimeout();
                    long latency = MathUtils.elapsedNanos(startTime);
                    if (rc != BKException.Code.OK) {
                        readLacOpLogger.registerFailedEvent(latency, TimeUnit.NANOSECONDS);
                    } else {
                        readLacOpLogger.registerSuccessfulEvent(latency, TimeUnit.NANOSECONDS);
                    }
                    originalCallback.readLacComplete(rc, ledgerId, lacBuffer, lastEntryBuffer, ctx);
                }
            };
        }
    }

    // visible for testing
    static class ReadCompletion extends CompletionValue {
        final ReadEntryCallback cb;

        public ReadCompletion(final PerChannelBookieClient pcbc, ReadEntryCallback cb, Object ctx,
                              long ledgerId, long entryId) {
            this(pcbc, null, cb, ctx, ledgerId, entryId, null);
        }

        public ReadCompletion(final PerChannelBookieClient pcbc, final OpStatsLogger readEntryOpLogger,
                              final ReadEntryCallback originalCallback,
                              final Object originalCtx, final long ledgerId, final long entryId,
                              final Timeout timeout) {
            super(originalCtx, ledgerId, entryId, timeout);
            final long startTime = MathUtils.nowInNano();
            this.cb = new ReadEntryCallback() {
                @Override
                public void readEntryComplete(int rc, long ledgerId, long entryId, ByteBuf buffer, Object ctx) {
                    cancelTimeout();
                    if (readEntryOpLogger != null) {
                        long latency = MathUtils.elapsedNanos(startTime);
                        if (rc != BKException.Code.OK) {
                            readEntryOpLogger.registerFailedEvent(latency, TimeUnit.NANOSECONDS);
                        } else {
                            readEntryOpLogger.registerSuccessfulEvent(latency, TimeUnit.NANOSECONDS);
                        }
                    }

                    if (rc != BKException.Code.OK && !expectedBkOperationErrors.contains(rc)) {
                        pcbc.recordError();
                    }

                    originalCallback.readEntryComplete(rc, ledgerId, entryId, buffer, originalCtx);
                }
            };
        }
    }

    static class StartTLSCompletion extends CompletionValue {
        final StartTLSCallback cb;

        public StartTLSCompletion(final PerChannelBookieClient pcbc, StartTLSCallback cb, Object ctx) {
            this(pcbc, null, cb, ctx, null);
        }

        public StartTLSCompletion(final PerChannelBookieClient pcbc, final OpStatsLogger startTLSOpLogger,
                                  final StartTLSCallback originalCallback, final Object originalCtx, final Timeout timeout) {
            super(originalCtx, -1, -1, timeout);
            final long startTime = MathUtils.nowInNano();
            this.cb = new StartTLSCallback() {
                @Override
                public void startTLSComplete(int rc, Object ctx) {
                    cancelTimeout();
                    if (startTLSOpLogger != null) {
                        long latency = MathUtils.elapsedNanos(startTime);
                        if (rc != BKException.Code.OK) {
                            startTLSOpLogger.registerFailedEvent(latency, TimeUnit.NANOSECONDS);
                        } else {
                            startTLSOpLogger.registerSuccessfulEvent(latency, TimeUnit.NANOSECONDS);
                        }
                    }

                    if (rc != BKException.Code.OK && !expectedBkOperationErrors.contains(rc)) {
                        pcbc.recordError();
                    }

                    if (originalCallback != null) {
                        originalCallback.startTLSComplete(rc, originalCtx);
                    }
                }
            };
        }
    }

    // visible for testing
    static class GetBookieInfoCompletion extends CompletionValue {
        final GetBookieInfoCallback cb;

        public GetBookieInfoCompletion(final PerChannelBookieClient pcbc, GetBookieInfoCallback cb, Object ctx) {
            this(pcbc, null, cb, ctx, null);
        }

        public GetBookieInfoCompletion(final PerChannelBookieClient pcbc, final OpStatsLogger getBookieInfoOpLogger,
                              final GetBookieInfoCallback originalCallback,
                              final Object originalCtx, final Timeout timeout) {
            super(originalCtx, 0L, 0L, timeout);
            final long startTime = MathUtils.nowInNano();
            this.cb = (null == getBookieInfoOpLogger) ? originalCallback : new GetBookieInfoCallback() {
                @Override
                public void getBookieInfoComplete(int rc, BookieInfo bInfo, Object ctx) {
                    cancelTimeout();
                    if (getBookieInfoOpLogger != null) {
                        long latency = MathUtils.elapsedNanos(startTime);
                        if (rc != BKException.Code.OK) {
                            getBookieInfoOpLogger.registerFailedEvent(latency, TimeUnit.NANOSECONDS);
                        } else {
                            getBookieInfoOpLogger.registerSuccessfulEvent(latency, TimeUnit.NANOSECONDS);
                        }
                    }

                    if (rc != BKException.Code.OK && !expectedBkOperationErrors.contains(rc)) {
                        pcbc.recordError();
                    }

                    originalCallback.getBookieInfoComplete(rc, bInfo, originalCtx);
                }
            };
        }
    }

    // visible for testing
    static class AddCompletion extends CompletionValue {
        final WriteCallback cb;

        public AddCompletion(final PerChannelBookieClient pcbc, WriteCallback cb, Object ctx,
                             long ledgerId, long entryId) {
            this(pcbc, null, cb, ctx, ledgerId, entryId, null);
        }

        public AddCompletion(final PerChannelBookieClient pcbc, final OpStatsLogger addEntryOpLogger,
                             final WriteCallback originalCallback,
                             final Object originalCtx, final long ledgerId, final long entryId,
                             final Timeout timeout) {
            super(originalCtx, ledgerId, entryId, timeout);
            final long startTime = MathUtils.nowInNano();
            this.cb = null == addEntryOpLogger ? originalCallback : new WriteCallback() {
                @Override
                public void writeComplete(int rc, long ledgerId, long entryId, BookieSocketAddress addr, Object ctx) {
                    cancelTimeout();
                    if (pcbc.addEntryOpLogger != null) {
                        long latency = MathUtils.elapsedNanos(startTime);
                        if (rc != BKException.Code.OK) {
                            pcbc.addEntryOpLogger.registerFailedEvent(latency, TimeUnit.NANOSECONDS);
                        } else {
                            pcbc.addEntryOpLogger.registerSuccessfulEvent(latency, TimeUnit.NANOSECONDS);
                        }
                    }

                    if (rc != BKException.Code.OK && !expectedBkOperationErrors.contains(rc)) {
                        pcbc.recordError();
                    }

                    originalCallback.writeComplete(rc, ledgerId, entryId, addr, originalCtx);
                }
            };
        }
    }

    // visable for testing
    CompletionKey newCompletionKey(long txnId, OperationType operationType) {
        return new CompletionKey(txnId, operationType);
    }

    Timeout scheduleTimeout(CompletionKey key, long timeout) {
        if (null != requestTimer) {
            return requestTimer.newTimeout(key, timeout, TimeUnit.SECONDS);
        } else {
            return null;
        }
    }

    class CompletionKey implements TimerTask {
        final long txnId;
        final OperationType operationType;
        final long requestAt;

        CompletionKey(long txnId, OperationType operationType) {
            this.txnId = txnId;
            this.operationType = operationType;
            this.requestAt = MathUtils.nowInNano();
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof CompletionKey)) {
                return false;
            }
            CompletionKey that = (CompletionKey) obj;
            return this.txnId == that.txnId && this.operationType == that.operationType;
        }

        @Override
        public int hashCode() {
            return ((int) txnId);
        }

        @Override
        public String toString() {
            return String.format("TxnId(%d), OperationType(%s)", txnId, operationType);
        }

        private long elapsedTime() {
            return MathUtils.elapsedNanos(requestAt);
        }

        @Override
        public void run(Timeout timeout) throws Exception {
            if (timeout.isCancelled()) {
                return;
            }
            if (OperationType.ADD_ENTRY == operationType) {
                errorOutAddKey(this, BKException.Code.TimeoutException);
                addEntryOutstanding.dec();
                addTimeoutOpLogger.registerSuccessfulEvent(elapsedTime(), TimeUnit.NANOSECONDS);
            } else if (OperationType.READ_ENTRY == operationType) {
                errorOutReadKey(this, BKException.Code.TimeoutException);
                readTimeoutOpLogger.registerSuccessfulEvent(elapsedTime(), TimeUnit.NANOSECONDS);
                readEntryOutstanding.dec();
            } else if (OperationType.WRITE_LAC == operationType) {
                errorOutWriteLacKey(this, BKException.Code.TimeoutException);
                writeLacTimeoutOpLogger.registerSuccessfulEvent(elapsedTime(), TimeUnit.NANOSECONDS);
            } else if (OperationType.GET_BOOKIE_INFO == operationType) {
                errorOutGetBookieInfoKey(this, BKException.Code.TimeoutException);
                getBookieInfoTimeoutOpLogger.registerSuccessfulEvent(elapsedTime(), TimeUnit.NANOSECONDS);
            } else if (OperationType.START_TLS == operationType) {
                errorStartTLS(BKException.Code.TimeoutException);
            } else {
                errorOutReadLacKey(this, BKException.Code.TimeoutException);
                readLacTimeoutOpLogger.registerSuccessfulEvent(elapsedTime(), TimeUnit.NANOSECONDS);
            }
        }
    }


    /**
     * Note : Helper functions follow
     */

    /**
     * @param status
     * @return null if the statuscode is unknown.
     */
    private Integer statusCodeToExceptionCode(StatusCode status) {
        Integer rcToRet = null;
        switch (status) {
            case EOK:
                rcToRet = BKException.Code.OK;
                break;
            case ENOENTRY:
                rcToRet = BKException.Code.NoSuchEntryException;
                break;
            case ENOLEDGER:
                rcToRet = BKException.Code.NoSuchLedgerExistsException;
                break;
            case EBADVERSION:
                rcToRet = BKException.Code.ProtocolVersionException;
                break;
            case EUA:
                rcToRet = BKException.Code.UnauthorizedAccessException;
                break;
            case EFENCED:
                rcToRet = BKException.Code.LedgerFencedException;
                break;
            case EREADONLY:
                rcToRet = BKException.Code.WriteOnReadOnlyBookieException;
                break;
            default:
                break;
        }
        return rcToRet;
    }

    private long getTxnId() {
        return txnIdGenerator.incrementAndGet();
    }

    public class ConnectionFutureListener implements ChannelFutureListener {
        private final long startTime;

        ConnectionFutureListener(long startTime) {
            this.startTime = startTime;
        }

        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
            LOG.debug("Channel connected ({}) {}", future.isSuccess(), future.channel());
            int rc;
            Queue<GenericCallback<PerChannelBookieClient>> oldPendingOps;

            /* We fill in the timer based on whether the connect operation itself succeeded regardless of
             * whether there was a race */
            if (future.isSuccess()) {
                PerChannelBookieClient.this.connectTimer.registerSuccessfulEvent(MathUtils.elapsedNanos(startTime), TimeUnit.NANOSECONDS);
            } else {
                PerChannelBookieClient.this.connectTimer.registerFailedEvent(MathUtils.elapsedNanos(startTime), TimeUnit.NANOSECONDS);
            }

            synchronized (PerChannelBookieClient.this) {
                if (future.isSuccess() && state == ConnectionState.CONNECTING) {
                    LOG.info("Successfully connected to bookie: {}", future.channel());
                    rc = BKException.Code.OK;
                    channel = future.channel();
                    if (shFactory != null) {
                        initiateSSL();
                        channelChanged();
                        return;
                    } else {
                        LOG.info("Successfully connected to bookie: " + addr);
                        state = ConnectionState.CONNECTED;
                        channelChanged();
                    }
                } else if (future.isSuccess() && state == ConnectionState.START_TLS) {
                    rc = BKException.Code.OK;
                    LOG.info("Successfully connected to bookie using SSL: " + addr);

                    state = ConnectionState.CONNECTED;
                    AuthHandler.ClientSideHandler authHandler = future.channel().pipeline()
                            .get(AuthHandler.ClientSideHandler.class);
                    authHandler.authProvider.onProtocolUpgrade();
                } else if (future.isSuccess() && (state == ConnectionState.CLOSED
                    || state == ConnectionState.DISCONNECTED)) {
                    LOG.warn("Closed before connection completed, clean up: {}, current state {}",
                            future.channel(), state);
                    closeChannel(future.channel());
                    rc = BKException.Code.BookieHandleNotAvailableException;
                    channel = null;
                } else if (future.isSuccess() && state == ConnectionState.CONNECTED) {
                    LOG.debug("Already connected with another channel({}), so close the new channel({})",
                            channel, future.channel());
                    closeChannel(future.channel());
                    return; // pendingOps should have been completed when other channel connected
                } else {
                    LOG.error("Could not connect to bookie: {}/{}, current state {} : ",
                            new Object[] { future.channel(), addr, state, future.cause() });
                    rc = BKException.Code.BookieHandleNotAvailableException;
                    closeChannel(future.channel());
                    channel = null;
                    if (state != ConnectionState.CLOSED) {
                        state = ConnectionState.DISCONNECTED;
                    }
                }

                // trick to not do operations under the lock, take the list
                // of pending ops and assign it to a new variable, while
                // emptying the pending ops by just assigning it to a new
                // list
                oldPendingOps = pendingOps;
                pendingOps = new ArrayDeque<>();
            }

            for (GenericCallback<PerChannelBookieClient> pendingOp : oldPendingOps) {
                pendingOp.operationComplete(rc, PerChannelBookieClient.this);
            }
        }
    }


    private void channelChanged() {
        synchronized (channelMonitor) {
            channelMonitor.notifyAll();
        }
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
        channelChanged();
        super.channelWritabilityChanged(ctx);
    }

    public ChannelFuture channelWrite(Object msg) {
        return channelWrite(msg, channelWaitOnWriteMillis);
    }

    public ChannelFuture channelWrite(Object msg, long timeoutMillis) {
        Channel c = channel;
        if (c != null && !c.isWritable() && timeoutMillis > 0) {
            if (fastFailEnabled) {
                LOG.info("Failing consquent channelWrite attempt to non-writable channel");
                sendWaitTimer.registerFailedEvent(0L, TimeUnit.NANOSECONDS);
                return new DefaultChannelPromise(c).setFailure(failingConsequentWriteEx);
            }

            final long startTime = MathUtils.nowInNano();
            final long maxSleepUntil = startTime + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
            synchronized (channelMonitor) {
                c = channel;
                while (c != null && !c.isWritable()) {
                    final long nowInNanos = MathUtils.nowInNano();
                    if (!c.isActive()) {
                        sendWaitTimer.registerFailedEvent(MathUtils.elapsedNanos(startTime), TimeUnit.NANOSECONDS);
                        return new DefaultChannelPromise(c)
                                .setFailure(new Exception("cannot write to inactive channel, PCBC state = " + state));
                    } else if (nowInNanos > maxSleepUntil) {
                        // fast fail writes until channel becomes writable
                        fastFailEnabled = true;
                        sendWaitTimer.registerFailedEvent(MathUtils.elapsedNanos(startTime), TimeUnit.NANOSECONDS);
                        return new DefaultChannelPromise(c).setFailure(notWritableTooLongEx);
                    }
                    try {
                        long sleepFor = TimeUnit.NANOSECONDS.toMillis(maxSleepUntil - nowInNanos);
                        sleepFor = (sleepFor < 1) ? 1 : sleepFor;
                        if (sleepFor > 1000) {
                            // I encountered case when no events get triggered to call channelMonitor.notify()
                            // Overall it looks like there is a condition (possibly netty bug) when 
                            // channelWritabilityChanged is not fired and so are other available events. 
                            // I've tried other events that ChannelInboundHandlerAdapter provides but without any luck.
                            // The only reliable solution I found so far is to force wake thread even when channel 
                            // is not being connected/disconnected/channelWritabilityChanged.
                            sleepFor = 1000;
                        }
                        channelMonitor.wait(sleepFor);
                    } catch (InterruptedException ie) {
                        sendWaitTimer.registerFailedEvent(MathUtils.elapsedNanos(startTime), TimeUnit.NANOSECONDS);
                        Thread.currentThread().interrupt();
                        return new DefaultChannelPromise(c).setFailure(ie);
                    }
                    c = channel;
                } //while
            }
            final long elapsed = MathUtils.elapsedNanos(startTime);
            sendWaitTimer.registerSuccessfulEvent(elapsed, TimeUnit.NANOSECONDS);
            if (elapsed > TimeUnit.MILLISECONDS.toNanos(1)) {
                LOG.warn("Spent {} milliseconds waiting for the channel {} to become writable", 
                        TimeUnit.NANOSECONDS.toMillis(elapsed), channel);
            }
        }

        fastFailEnabled = false;

        c = channel;
        if (c == null) {
            return new DefaultChannelPromise(c)
                    .setFailure(new Exception("cannot write to disconnected channel, PCBC state = " + state));
        }

        return c.writeAndFlush(msg);
    }

    private void initiateSSL() {
        LOG.info("Initializing SSL to {}",channel);
        assert state == ConnectionState.CONNECTING;
        final long txnId = getTxnId();
        final CompletionKey completionKey = new CompletionKey(txnId, OperationType.START_TLS);
        completionObjects.put(completionKey, new StartTLSCompletion(this, startTLSOpLogger, null, null,
                scheduleTimeout(completionKey, startTLSTimeout)));
        BookkeeperProtocol.Request.Builder h = BookkeeperProtocol.Request.newBuilder();
        BKPacketHeader.Builder headerBuilder = BKPacketHeader.newBuilder()
                .setVersion(ProtocolVersion.VERSION_THREE)
                .setOperation(OperationType.START_TLS)
                .setTxnId(txnId);
        h.setHeader(headerBuilder.build());
        h.setStartTLSRequest(BookkeeperProtocol.StartTLSRequest.newBuilder().build());
        state = ConnectionState.START_TLS;
        ChannelFuture future = channelWrite(h.build());
        future.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                if (!future.isSuccess()) {
                    LOG.error("Failed to send START_TLS request");
                    failSSL(BKException.Code.SecurityException);
                }
            }
        });
    }
    private void failSSL(int rc) {
        LOG.error("SSL failure on: {}, rc: {}", channel, rc);
        Queue<GenericCallback<PerChannelBookieClient>> oldPendingOps;
        synchronized(this) {
            disconnect(false);
            oldPendingOps = pendingOps;
            pendingOps = new ArrayDeque<>();
        }
        for (GenericCallback<PerChannelBookieClient> pendingOp : oldPendingOps) {
            pendingOp.operationComplete(rc, null);
        }
    }
}
