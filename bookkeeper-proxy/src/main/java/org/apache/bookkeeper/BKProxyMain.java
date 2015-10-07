package org.apache.bookkeeper;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.conf.BookKeeperProxyConfiguraiton;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.zookeeper.KeeperException;
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
    private static AtomicInteger threadNum = new AtomicInteger();
    private final static Logger LOG = LoggerFactory.getLogger(BKProxyMain.class);

    private ServerSocketChannel serverChannel;
    private final BookKeeperProxyConfiguraiton bkpConf;

    public BKProxyMain(BookKeeperProxyConfiguraiton bkpConf) {
        this.bkpConf = bkpConf;
    }

    public BookKeeperProxyConfiguraiton getBookKeeperProxyConfiguraiton() {
        return bkpConf;
    }

    @Override
    public void run() {
        threadNum.set(0);
        try {
            serverChannel = ServerSocketChannel.open();
            serverChannel.setOption(java.net.StandardSocketOptions.SO_RCVBUF,
                    bkpConf.getServerChannelReceiveBufferSize());
            serverChannel.socket().bind(new InetSocketAddress(bkpConf.getBKProxyPort()));
            SocketChannel sock = null;

            // Global structures
            BookKeeper bk = null;
            BKExtentLedgerMap elm = new BKExtentLedgerMap();

            try {
                bk = new BookKeeper(bkpConf);
            } catch (InterruptedException ie) {
                // ignore
            } catch (KeeperException | IOException e) {
                LOG.error(e.toString());
                System.exit(1);
            }

            int workerThreadLimit = bkpConf.getWorkerThreadLimit();
            while (true) {
                System.out.println("SFStore BK-Proxy: Waiting for connection... ");
                sock = serverChannel.accept();

                if (threadNum.get() == workerThreadLimit) { // max connections
                    System.out.println("Bailing out!! Maximum Connections Reached: " + workerThreadLimit);
                    break;
                }

                // Worker thread to handle each backend's write requests
                new Thread(new BKProxyWorker(bkpConf, threadNum, sock, bk, elm)).start();

                threadNum.incrementAndGet();
            }
        } catch (IOException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
    }

    public void shutdown() throws IOException {
        if (serverChannel != null) {
            System.out.println("Stopping proxy...");
            serverChannel.close();
        }
    }

    public static void main(String args[]) throws Exception {
        BookKeeperProxyConfiguraiton bkpConfig = new BookKeeperProxyConfiguraiton();
        loadConfFile(bkpConfig, CONFIG_FILE);
        BKProxyMain bkMain = new BKProxyMain(bkpConfig);
        Thread bkMainThread = new Thread(bkMain);
        bkMainThread.start();
        bkMainThread.join();
    }

    /*
     * load configurations from file.
     */
    private static void loadConfFile(ClientConfiguration conf, String confFile) throws IllegalArgumentException {
        try {
            conf.loadConf(new File(confFile).toURI().toURL());
        } catch (MalformedURLException e) {
            LOG.error("Malformed configuration file: " + confFile, e);
            throw new IllegalArgumentException();
        } catch (ConfigurationException e) {
            LOG.error("Could not open configuration file: " + confFile, e);
            throw new IllegalArgumentException();
        }
        LOG.info("Using configuration file " + confFile);
    }
}
