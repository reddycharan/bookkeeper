package org.apache.bookkeeper;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.conf.BookKeeperProxyConfiguration;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * BKProxyMain implements runnable method and when Thread is spawned it starts ServerSocketChannel on the provided bkProxyPort.
 * Clients connect to bkProxyPort and the client channel is created on accept()
 * A new worker thread is generated for each client channel and that thread ends when client closes the connection
 *
 * The main() method starts BKProxyMain Thread on the well known port (5555) and waits for its completion.
 */
public class BKProxyMain implements Runnable {

    private static final String CONFIG_FILE = "conf/bk_client_proxy.conf";
    private AtomicInteger threadNum = new AtomicInteger();
    private final static Logger LOG = LoggerFactory.getLogger(BKProxyMain.class);

    private ServerSocketChannel serverChannel;
    private final BookKeeperProxyConfiguration bkpConf;

    public BKProxyMain(BookKeeperProxyConfiguration bkpConf) {
        this.bkpConf = bkpConf;
    }

    public BookKeeperProxyConfiguration getBookKeeperProxyConfiguration() {
        return bkpConf;
    }

    /*
     *  Get a thread slot even if there are multiple threads reading the value of threadNum at the same time.
     */
    private int getThreadSlot(int workerThreadLimit) {
        int numThreads = 0;
        while ((numThreads = threadNum.get()) < workerThreadLimit) {
            if (threadNum.compareAndSet(numThreads, numThreads + 1)) {
                return (numThreads + 1);
            }
        }
        return -1;
    }

    @Override
    public void run() {
        threadNum.set(0);
        try {
            serverChannel = ServerSocketChannel.open();
            serverChannel.setOption(java.net.StandardSocketOptions.SO_RCVBUF,
                    bkpConf.getServerChannelReceiveBufferSize());
            serverChannel.socket().bind(new InetSocketAddress(bkpConf.getBKProxyPort()));

            // Global structures
            BookKeeper bk = null;
            BKExtentLedgerMap elm = new BKExtentLedgerMap();

            try {
                bk = new BookKeeper(bkpConf);
            } catch (Exception e) {
                LOG.error("Exception while creating a new Bookkeeper:", e);
                throw e;
            }

            int workerThreadLimit = bkpConf.getWorkerThreadLimit();
            while (true) {
                LOG.debug("SFStore BK-Proxy: Waiting for connection... ");

                SocketChannel sock = null;
                try {
                    sock = serverChannel.accept();

                    int threadSlot = getThreadSlot(workerThreadLimit);
                    if (threadSlot == -1) { // max connections
                        LOG.warn("Maximum Connections Reached: " + workerThreadLimit
                                + ". Hence not accepting a new connection!");
                        continue;
                    }

                    // Worker thread to handle each backend's write requests
                    try {
                        Thread worker = new Thread(new BKProxyWorker(bkpConf, threadNum, sock, bk, elm));
                        sock = null;
                        worker.start();
                    } catch (Exception e) {
                        LOG.error("Exception in constructing new proxy worker:", e);
                        // worker decrements count even on failed construction
                    }
                } finally {
                    if (sock != null) {
                        sock.close();
                        sock = null;
                    }
                }
            }
        } catch (Exception e) {
            LOG.error("Exception in starting a new bookkeeper proxy thread:", e);
            return;
        }
    }

    public void shutdown() throws IOException {
        if (serverChannel == null) {
            return;
        }

        LOG.info("Stopping proxy...");
        serverChannel.close();
    }

    public static void main(String args[]) throws Exception {
        BookKeeperProxyConfiguration bkpConfig = new BookKeeperProxyConfiguration();
        try {
            loadConfFile(bkpConfig, CONFIG_FILE);
            BKProxyMain bkMain = new BKProxyMain(bkpConfig);
            Thread bkWorkerThreadController = new Thread(bkMain);
            bkWorkerThreadController.start();
            bkWorkerThreadController.join();
        } catch (Exception e) {
            LOG.error("Exception in main thread:", e);
            throw e;
        }
    }

    /*
     * load configurations from file.
     */
    private static void loadConfFile(ClientConfiguration conf, String confFile) throws Exception {
        try {
            conf.loadConf(new File(confFile).toURI().toURL());
        } catch (MalformedURLException e) {
            LOG.error("Malformed configuration file: " + confFile, e);
            throw e;
        } catch (ConfigurationException e) {
            LOG.error("Could not open configuration file: " + confFile, e);
            throw e;
        } catch (Exception e) {
            LOG.error("Exception while loading configuration file:", e);
            throw e;
        }

        LOG.info("Using configuration file " + confFile);
    }
}
