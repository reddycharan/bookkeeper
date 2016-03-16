package org.apache.bookkeeper.conf;

import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BookKeeperProxyConfiguration extends ClientConfiguration {
    private static final Logger logger = LoggerFactory.getLogger(BookKeeperProxyConfiguration.class);

    protected static final String BKPROXY_HOSTNAME = "bkProxyHostname";
    protected static final String BKPROXY_PORT = "bkProxyPort";
    protected static final String SERVERCHANNEL_RECEIVE_BUFFER_SIZE = "serverChannelReceiveBufferSize";
    protected static final String CLIENTCHANNEL_RECEIVE_BUFFER_SIZE = "clientChannelReceiveBufferSize";
    protected static final String CLIENTCHANNEL_SEND_BUFFER_SIZE = "clientChannelSendBufferSize";
    protected static final String WORKER_THREAD_LIMIT = "workerThreadLimit";
    protected static final String MAX_FRAG_SIZE = "maxFragSize";
    protected static final String TCP_NODELAY = "tcpNoDelay";
    protected static final String PASSWORD = "password";
    protected static final String ENSEMBLE_SIZE = "ensembleSize";
    protected static final String WRITE_QUORUM_SIZE = "writeQuorumSize";
    protected static final String ACK_QUORUM_SIZE = "ackQuorumSize";
    protected static final String DIGEST_TYPE = "digestType";

    private static final String MAC = "MAC";
    private static final String CRC32 = "CRC32";

    public BookKeeperProxyConfiguration() {
        super();
    }

    public BookKeeperProxyConfiguration(AbstractConfiguration conf) {
        super();
        loadConf(conf);
    }

    public String getBKProxyHostname() {
        return getString(BKPROXY_HOSTNAME, "localhost");
    }

    public BookKeeperProxyConfiguration setBKProxyHostname(String bkProxyHostname) {
        setProperty(BKPROXY_HOSTNAME, bkProxyHostname);
        return this;
    }

    public int getBKProxyPort() {
        return getInt(BKPROXY_PORT, 5555);
    }

    public BookKeeperProxyConfiguration setBKProxyPort(int bkProxyPort) {
        setProperty(BKPROXY_PORT, Integer.toString(bkProxyPort));
        return this;
    }

    public int getServerChannelReceiveBufferSize() {
        return getInt(SERVERCHANNEL_RECEIVE_BUFFER_SIZE, 65536);
    }

    public BookKeeperProxyConfiguration setServerChannelReceiveBufferSize(int serverChannelReceiveBufferSize) {
        setProperty(SERVERCHANNEL_RECEIVE_BUFFER_SIZE, serverChannelReceiveBufferSize);
        return this;
    }

    public int getClientChannelReceiveBufferSize() {
        return getInt(CLIENTCHANNEL_RECEIVE_BUFFER_SIZE, 65536);
    }

    public BookKeeperProxyConfiguration setClientChannelReceiveBufferSize(int clientChannelReceiveBufferSize) {
        setProperty(CLIENTCHANNEL_RECEIVE_BUFFER_SIZE, clientChannelReceiveBufferSize);
        return this;
    }

    public int getClientChannelSendBufferSize() {
        return getInt(CLIENTCHANNEL_SEND_BUFFER_SIZE, 65536);
    }

    public BookKeeperProxyConfiguration setClientChannelSendBufferSize(int clientChannelSendBufferSize) {
        setProperty(CLIENTCHANNEL_SEND_BUFFER_SIZE, clientChannelSendBufferSize);
        return this;
    }

    public int getWorkerThreadLimit() {
        return getInt(WORKER_THREAD_LIMIT, 1000);
    }

    public BookKeeperProxyConfiguration setWorkerThreadLimit(int workerThreadLimit) {
        setProperty(WORKER_THREAD_LIMIT, workerThreadLimit);
        return this;
    }

    public int getMaxFragSize() {
        return getInt(MAX_FRAG_SIZE, 1048576);
    }

    public BookKeeperProxyConfiguration setMaxFragSize(int maxFragSize) {
        setProperty(MAX_FRAG_SIZE, maxFragSize);
        return this;
    }

    public boolean getTCPNoDelay() {
        return getBoolean(TCP_NODELAY, true);
    }

    public BookKeeperProxyConfiguration setTCPNoDelay(boolean tcpNoDelay) {
        setProperty(TCP_NODELAY, Boolean.toString(tcpNoDelay));
        return this;
    }

    public String getPassword() {
        return getString(PASSWORD, "foo");
    }

    public BookKeeperProxyConfiguration setPassword(String password) {
        setProperty(PASSWORD, password);
        return this;
    }

    public int getEnsembleSize() {
        return getInt(ENSEMBLE_SIZE, 3);
    }

    public BookKeeperProxyConfiguration setEnsembleSize(int ensembleSize) {
        setProperty(ENSEMBLE_SIZE, ensembleSize);
        return this;
    }

    public int getWriteQuorumSize() {
        return getInt(WRITE_QUORUM_SIZE, 3);
    }

    public BookKeeperProxyConfiguration setWriteQuorumSize(int writeQuorumSize) {
        setProperty(WRITE_QUORUM_SIZE, writeQuorumSize);
        return this;
    }

    public int getAckQuorumSize() {
        return getInt(ACK_QUORUM_SIZE, 2);
    }

    public BookKeeperProxyConfiguration setAckQuorumSize(int ackQuorumSize) {
        setProperty(ACK_QUORUM_SIZE, ackQuorumSize);
        return this;
    }

    public DigestType getDigestType() {
        String digestTypeStr = getString(DIGEST_TYPE, MAC);
        if (digestTypeStr.equalsIgnoreCase(MAC)) {
            return DigestType.MAC;
        } else {
            return DigestType.CRC32;
        }
    }

    public BookKeeperProxyConfiguration setDigestType(DigestType digestType) {
        if (digestType == DigestType.MAC) {
            setProperty(DIGEST_TYPE, MAC);
        } else {
            setProperty(DIGEST_TYPE, CRC32);
        }
        return this;
    }
}
