package org.apache.bookkeeper.conf;

import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.stats.NullStatsProvider;
import org.apache.bookkeeper.stats.StatsProvider;
import org.apache.bookkeeper.util.ReflectionUtils;
import org.apache.commons.configuration.ConfigurationException;

public class BookKeeperProxyConfiguration extends ClientConfiguration {

    protected static final String BKPROXY_HOSTNAME_STR = "bkProxyHostname";
    protected static final String BKPROXY_HOSTNAME_DEF = "localhost";

    protected static final String BKPROXY_PORT_STR = "bkProxyPort";
    protected static final int BKPROXY_PORT_DEF = 5555;

    private static final int SIZE_64K = 64 * 1024;
    private static final int SIZE_1M = 1024 * 1024;

    protected static final String SERVERCHANNEL_RECEIVE_BUFFER_SIZE_STR = "serverChannelReceiveBufferSize";
    protected static final int SERVERCHANNEL_RECEIVE_BUFFER_SIZE_DEF = SIZE_64K;

    protected static final String CLIENTCHANNEL_RECEIVE_BUFFER_SIZE_STR = "clientChannelReceiveBufferSize";
    protected static final int CLIENTCHANNEL_RECEIVE_BUFFER_SIZE_DEF = SIZE_64K;

    protected static final String CLIENTCHANNEL_SEND_BUFFER_SIZE_STR = "clientChannelSendBufferSize";
    protected static final int CLIENTCHANNEL_SEND_BUFFER_SIZE_DEF = SIZE_64K;

    protected static final String WORKER_THREAD_LIMIT_STR = "workerThreadLimit";
    protected static final int WORKER_THREAD_LIMIT_DEF = 1000;

    protected static final String MAX_FRAG_SIZE_STR = "maxFragSize";
    protected static final int MAX_FRAG_SIZE_DEF = SIZE_1M;

    protected static final String TCP_NODELAY_STR = "tcpNoDelay";
    protected static final boolean TCP_NODELAY_DEF = true;

    protected static final String PASSWORD_STR = "password";
    protected static final String PASSWORD_DEF = "foo";

    protected static final String ENSEMBLE_SIZE_STR = "ensembleSize";
    protected static final int ENSEMBLE_SIZE_DEF = 3;

    protected static final String WRITE_QUORUM_SIZE_STR = "writeQuorumSize";
    protected static final int WRITE_QUORUM_SIZE_DEF = 3;

    protected static final String ACK_QUORUM_SIZE_STR = "ackQuorumSize";
    protected static final int ACK_QUORUM_SIZE_DEF = 2;

    private static final String DIGEST_MAC = "MAC";
    private static final String DIGEST_CRC32 = "CRC32";
    private static final String DIGEST_DUMMY = "DUMMY";

    protected static final String DIGEST_TYPE_STR = "digestType";
    protected static final String DIGEST_TYPE_DEF = DIGEST_CRC32;

    protected static final String CORE_POOL_SIZE_STR = "corePoolSize";
    protected static final int CORE_POOL_SIZE_DEF = 10;

    protected static final String CORE_POOL_KEEP_ALIVE_TIME_STR = "corePoolKeepAliveTime";
    protected static final long CORE_POOL_KEEP_ALIVE_TIME_DEF = 5000;

    protected static final String READ_HANDLE_CACHE_LEN_STR = "readHandleCacheLen";
    protected static final long READ_HANDLE_CACHE_LEN_DEF = 3 * 1000; // 3K handles

    protected static final String READ_HANDLE_TTL_STR = "readHandleTTL";
    protected static final long READ_HANDLE_TTL_DEF = -1; // unlimited
    
    // max # of byteBuffers in the shared pool; idle and available to be borrowed, puts an upperbound on unused space
    protected static final String BYTE_BUFFER_POOL_SIZE_MAX_IDLE_STR = "byteBufferPoolSizeMaxIdle";
    protected static final int BYTE_BUFFER_POOL_SIZE_MAX_IDLE_DEF = 100;

    protected static final String STATS_PROVIDER_CLASS = "statsProviderClass";
    protected static final String CODAHALE_STATS_OUTPUT_FREQUENCY="codahaleStatsOutputFrequency";
    protected static final String CODAHALE_STATS_CSV_ENDPOINT = "codahaleStatsCSVEndpoint";
    protected static final String SERVLET_CONTEXT_PATH="servletContextPath";
    protected static final String SERVLET_ENDPOINT = "servletEndpoint";

    public BookKeeperProxyConfiguration() {
        super();
    }

    public BookKeeperProxyConfiguration(AbstractConfiguration conf) {
        super();
        loadConf(conf);
    }
    
    public String getBKProxyHostname() {
        return getString(BKPROXY_HOSTNAME_STR, BKPROXY_HOSTNAME_DEF);
    }

    public BookKeeperProxyConfiguration setBKProxyHostname(String bkProxyHostname) {
        setProperty(BKPROXY_HOSTNAME_STR, bkProxyHostname);
        return this;
    }

    public int getBKProxyPort() {
        return getInt(BKPROXY_PORT_STR, BKPROXY_PORT_DEF);
    }

    public BookKeeperProxyConfiguration setBKProxyPort(int bkProxyPort) {
        setProperty(BKPROXY_PORT_STR, Integer.toString(bkProxyPort));
        return this;
    }

    public int getServerChannelReceiveBufferSize() {
        return getInt(SERVERCHANNEL_RECEIVE_BUFFER_SIZE_STR, SERVERCHANNEL_RECEIVE_BUFFER_SIZE_DEF);
    }

    public BookKeeperProxyConfiguration setServerChannelReceiveBufferSize(int serverChannelReceiveBufferSize) {
        setProperty(SERVERCHANNEL_RECEIVE_BUFFER_SIZE_STR, serverChannelReceiveBufferSize);
        return this;
    }

    public int getClientChannelReceiveBufferSize() {
        return getInt(CLIENTCHANNEL_RECEIVE_BUFFER_SIZE_STR, CLIENTCHANNEL_RECEIVE_BUFFER_SIZE_DEF);
    }

    public BookKeeperProxyConfiguration setClientChannelReceiveBufferSize(int clientChannelReceiveBufferSize) {
        setProperty(CLIENTCHANNEL_RECEIVE_BUFFER_SIZE_STR, clientChannelReceiveBufferSize);
        return this;
    }

    public int getClientChannelSendBufferSize() {
        return getInt(CLIENTCHANNEL_SEND_BUFFER_SIZE_STR, CLIENTCHANNEL_SEND_BUFFER_SIZE_DEF);
    }

    public BookKeeperProxyConfiguration setClientChannelSendBufferSize(int clientChannelSendBufferSize) {
        setProperty(CLIENTCHANNEL_SEND_BUFFER_SIZE_STR, clientChannelSendBufferSize);
        return this;
    }

    public int getWorkerThreadLimit() {
        return getInt(WORKER_THREAD_LIMIT_STR, WORKER_THREAD_LIMIT_DEF);
    }

    public BookKeeperProxyConfiguration setWorkerThreadLimit(int workerThreadLimit) {
        setProperty(WORKER_THREAD_LIMIT_STR, workerThreadLimit);
        return this;
    }

    public int getMaxFragSize() {
        return getInt(MAX_FRAG_SIZE_STR, MAX_FRAG_SIZE_DEF);
    }

    public BookKeeperProxyConfiguration setMaxFragSize(int maxFragSize) {
        setProperty(MAX_FRAG_SIZE_STR, maxFragSize);
        return this;
    }

    public boolean getTCPNoDelay() {
        return getBoolean(TCP_NODELAY_STR, TCP_NODELAY_DEF);
    }

    public BookKeeperProxyConfiguration setTCPNoDelay(boolean tcpNoDelay) {
        setProperty(TCP_NODELAY_STR, Boolean.toString(tcpNoDelay));
        return this;
    }

    public String getPassword() {
        return getString(PASSWORD_STR, PASSWORD_DEF);
    }

    public BookKeeperProxyConfiguration setPassword(String password) {
        setProperty(PASSWORD_STR, password);
        return this;
    }

    public int getEnsembleSize() {
        return getInt(ENSEMBLE_SIZE_STR, ENSEMBLE_SIZE_DEF);
    }

    public BookKeeperProxyConfiguration setEnsembleSize(int ensembleSize) {
        setProperty(ENSEMBLE_SIZE_STR, ensembleSize);
        return this;
    }

    public int getWriteQuorumSize() {
        return getInt(WRITE_QUORUM_SIZE_STR, WRITE_QUORUM_SIZE_DEF);
    }

    public BookKeeperProxyConfiguration setWriteQuorumSize(int writeQuorumSize) {
        setProperty(WRITE_QUORUM_SIZE_STR, writeQuorumSize);
        return this;
    }

    public int getAckQuorumSize() {
        return getInt(ACK_QUORUM_SIZE_STR, ACK_QUORUM_SIZE_DEF);
    }

    public BookKeeperProxyConfiguration setAckQuorumSize(int ackQuorumSize) {
        setProperty(ACK_QUORUM_SIZE_STR, ackQuorumSize);
        return this;
    }

    public DigestType getDigestType() {
        String digestTypeStr = getString(DIGEST_TYPE_STR, DIGEST_TYPE_DEF);
        if (digestTypeStr.equalsIgnoreCase(DIGEST_MAC)) {
            return DigestType.MAC;
        } else if (digestTypeStr.equalsIgnoreCase(DIGEST_DUMMY)) {
            return DigestType.DUMMY;
        } else {
            return DigestType.CRC32;
        }
    }

    public BookKeeperProxyConfiguration setDigestType(DigestType digestType) {
        if (digestType == DigestType.MAC) {
            setProperty(DIGEST_TYPE_STR, DIGEST_MAC);
        } else if (digestType == DigestType.DUMMY) {
            setProperty(DIGEST_TYPE_STR, DIGEST_DUMMY);
        } else {
            setProperty(DIGEST_TYPE_STR, DIGEST_CRC32);
        }
        return this;
    }

    public int getCorePoolSize() {
        return getInt(CORE_POOL_SIZE_STR, CORE_POOL_SIZE_DEF);
    }

    public BookKeeperProxyConfiguration setCorePoolSize(int corePoolSize) {
        setProperty(CORE_POOL_SIZE_STR, corePoolSize);
        return this;
    }

    public long getCorePoolKeepAliveTime() {
        return getLong(CORE_POOL_KEEP_ALIVE_TIME_STR, CORE_POOL_KEEP_ALIVE_TIME_DEF);
    }

    public BookKeeperProxyConfiguration setCorePoolKeepAliveTime(long corePoolKeepAliveTime) {
        setProperty(CORE_POOL_KEEP_ALIVE_TIME_STR, corePoolKeepAliveTime);
        return this;
    }

    public long getReadHandleCacheLen() {
        return getLong(READ_HANDLE_CACHE_LEN_STR, READ_HANDLE_CACHE_LEN_DEF);
    }

    public BookKeeperProxyConfiguration setReadHandleCacheLen(long readHandleCacheLen) {
        setProperty(READ_HANDLE_CACHE_LEN_STR, readHandleCacheLen);
        return this;
    }

    public long getReadHandleTTL() {
        return getLong(READ_HANDLE_TTL_STR, READ_HANDLE_TTL_DEF);
    }

    public BookKeeperProxyConfiguration setReadHandleTTL(long readHandleTTL) {
        setProperty(READ_HANDLE_TTL_STR, readHandleTTL);
        return this;
    }

    public BookKeeperProxyConfiguration setByteBufferPoolSizeMaxIdle(int byteBufferPoolSizeMaxIdle) {
        setProperty(BYTE_BUFFER_POOL_SIZE_MAX_IDLE_STR, byteBufferPoolSizeMaxIdle);
        return this;
    }

    public int getByteBufferPoolSizeMaxIdle() {
        return getInt(BYTE_BUFFER_POOL_SIZE_MAX_IDLE_STR, BYTE_BUFFER_POOL_SIZE_MAX_IDLE_DEF);
    }

    public Class<? extends StatsProvider> getStatsProviderClass()
        throws ConfigurationException {
        return ReflectionUtils.getClass(this, STATS_PROVIDER_CLASS,
                                        NullStatsProvider.class, StatsProvider.class,
                                        defaultLoader);
    }
}
