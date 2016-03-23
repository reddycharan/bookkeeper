package org.apache.bookkeeper.conf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.bookkeeper.client.BookKeeper.DigestType;

public class BookKeeperProxyConfiguration extends ClientConfiguration {
    private static final Logger logger = LoggerFactory.getLogger(BookKeeperProxyConfiguration.class);

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

    private static final String MAC = "MAC";
    private static final String CRC32 = "CRC32";

    protected static final String DIGEST_TYPE_STR = "digestType";
    protected static final String DIGEST_TYPE_DEF = MAC;

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
        if (digestTypeStr.equalsIgnoreCase(MAC)) {
            return DigestType.MAC;
        } else {
            return DigestType.CRC32;
        }
    }

    public BookKeeperProxyConfiguration setDigestType(DigestType digestType) {
        if (digestType == DigestType.MAC) {
            setProperty(DIGEST_TYPE_STR, MAC);
        } else {
            setProperty(DIGEST_TYPE_STR, CRC32);
        }
        return this;
    }
}
