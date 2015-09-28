package org.apache.bookkeeper;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.bookkeeper.client.BookKeeper;
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
    public static String bkserver = "localhost:2181";
    private static AtomicInteger threadNum = new AtomicInteger();
    private final static Logger LOG = LoggerFactory.getLogger(BKProxyMain.class);

    private int bkProxyPort;
    private ServerSocketChannel serverChannel;

    public BKProxyMain(int bkProxyPort) {
        this.bkProxyPort = bkProxyPort;
    }

    public int getBkProxyPort() {
        return bkProxyPort;
    }

    @Override
    public void run() {
        threadNum.set(0);
        try {
            serverChannel = ServerSocketChannel.open();
            serverChannel.setOption(java.net.StandardSocketOptions.SO_RCVBUF, 65536);
            serverChannel.socket().bind(new InetSocketAddress(bkProxyPort));
            SocketChannel sock = null;

            // Global structures
            BookKeeper bk = null;
            BKExtentLedgerMap elm = new BKExtentLedgerMap();

            try {
                bk = new BookKeeper(bkserver);
            } catch (InterruptedException ie) {
                // ignore
            } catch (KeeperException | IOException e) {
                LOG.error(e.toString());
                System.exit(1);
            }

            while (true) {
                System.out.println("SFStore BK-Proxy: Waiting for connection... ");
                sock = serverChannel.accept();

                if (threadNum.get() == BKPConstants.WORKER_THREAD_LIMIT) { // max connections
                    System.out.println("Bailing out!! Maximum Connections Reached: " + BKPConstants.WORKER_THREAD_LIMIT);
                    break;
                }

                // Worker thread to handle each backend's write requests
                new Thread(new BKProxyWorker(threadNum, sock, bk, elm)).start();

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
        BKProxyMain bkMain = new BKProxyMain(5555);
        Thread bkMainThread = new Thread(bkMain);
        bkMainThread.start();
        bkMainThread.join();
    }
}
