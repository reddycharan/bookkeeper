package org.apache.bookkeeper;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.conf.BookKeeperProxyConfiguration;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.io.output.ByteArrayOutputStream;

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
    private final static Logger LOG = LoggerFactory.getLogger(BKProxyMain.class);

    private static final String DEFAULT_CONFIG_FILE = "conf/bk_client_proxy.conf";

    private static final String OPT_HELP_FLAG        = "h";
    private static final String OPT_ZK_SERVERS       = "z";
    private static final String OPT_CONFIG_FILE      = "c";
    private static final String OPT_HOSTNAME         = "H";
    private static final String OPT_PORT             = "p";

    private static String  configFile;
    private static Options options;

    private AtomicInteger threadNum = new AtomicInteger();
    private ServerSocketChannel serverChannel;
    private final BookKeeperProxyConfiguration bkpConf;
    private BookKeeper bk = null;

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
            String hostname = bkpConf.getBKProxyHostname();
            int port = bkpConf.getBKProxyPort();
            serverChannel.socket().bind(new InetSocketAddress(hostname, port));

            // Global structures
            BKExtentLedgerMap elm = new BKExtentLedgerMap();

            try {
                bk = new BookKeeper(bkpConf);
            } catch (Exception e) {
                LOG.error("Exception while creating a new Bookkeeper:", e);
                throw e;
            }

            int workerThreadLimit = bkpConf.getWorkerThreadLimit();
            while (true) {
                LOG.info("SFStore BK-Proxy: Waiting for connection... ");
                // Let jenkins know that we are up.
                System.out.println("SFStore BK-Proxy: Waiting for connection... ");

                SocketChannel sock = null;
                try {
                    try {
                        sock = serverChannel.accept();
                    } catch (ClosedChannelException cce) {
                        // we can get this exception when serverChannel.close() is called
                        LOG.debug("Channel has been closed, so terminating main thread. Exception:", cce);
                        break;
                    }
                    String remoteAddress = "";
                    try {
                        remoteAddress = sock.getRemoteAddress().toString();
                        LOG.debug("Accepted connection from: {}", remoteAddress);
                    } catch (Exception e) {
                        LOG.error("Exception trying to get client address", e);
                    }

                    int threadSlot = getThreadSlot(workerThreadLimit);
                    if (threadSlot == -1) { // max connections
                        LOG.warn("Maximum Connections Reached: " + workerThreadLimit
                                + ". Hence not accepting a new connection from {}!",
                                remoteAddress);
                        continue;
                    }
                    String proxyIdentifier = "BKProxyWorker:[" + remoteAddress + "]:" + threadSlot;

                    // Worker thread to handle each backend's write requests
                    try {
                        Thread worker = new Thread(new BKProxyWorker(bkpConf, threadNum, sock, bk, elm),
                                            proxyIdentifier);
                        sock = null; // worker takes over handle and handles close
                        worker.start();
                    } catch (Exception e) {
                        LOG.error("Exception in constructing new proxy worker: {} ", proxyIdentifier, e);
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

    public void shutdown() throws IOException, InterruptedException, BKException {
        if (serverChannel != null) {
            System.out.println("Stopping proxy...");
            serverChannel.close();
        }
        if (bk != null){
            LOG.info("Closing BookKeeper...");
            bk.close();
        }
    }

    public static void main(String args[]) throws Exception {
        BookKeeperProxyConfiguration bkConfig = new BookKeeperProxyConfiguration();
        try {
            BookKeeperProxyConfiguration cmdLineConfig = new BookKeeperProxyConfiguration();
            BookKeeperProxyConfiguration fileConfig = new BookKeeperProxyConfiguration();

            if (parseArgs(args, cmdLineConfig)) {
                loadConfFile(fileConfig, configFile);

                // Compose the configuration files such that cmd line has precedence over the config file
                bkConfig.addConfiguration(cmdLineConfig);
                bkConfig.addConfiguration(fileConfig);

                BKProxyMain bkMain = new BKProxyMain(bkConfig);
                Thread bkWorkerThreadController = new Thread(bkMain, "BKProxyMain");
                bkWorkerThreadController.start();
                bkWorkerThreadController.join();
            }
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


    private static boolean parseArgs(String[] args, BookKeeperProxyConfiguration cmdLineConfig) {
        options = new Options();

        options.addOption(OPT_HELP_FLAG, "help", false, "print usage message");
        options.addOption(OPT_HOSTNAME, "hostname", true, "hostname/IP address to listen on");
        options.addOption(OPT_PORT, "port", true, "port to listen on");
        options.addOption(OPT_ZK_SERVERS, "zk-servers", true, "comma separated list of Zookeeper servers <hostname:port>");
        options.addOption(OPT_CONFIG_FILE, "config-file", true, "path to configuration file");

        try {
            CommandLineParser parser  = new GnuParser();
            CommandLine       cmdLine = parser.parse(options, args);
            boolean           helpFlag;

            helpFlag = cmdLine.hasOption(OPT_HELP_FLAG);
            if (helpFlag) {
                usage();
                return false;
            }

            if (cmdLine.hasOption(OPT_HOSTNAME)) {
                cmdLineConfig.setBKProxyHostname(cmdLine.getOptionValue(OPT_HOSTNAME));
            }

            if (cmdLine.hasOption(OPT_PORT)) {
                cmdLineConfig.setBKProxyHostname(cmdLine.getOptionValue(OPT_PORT));
            }

            if (cmdLine.hasOption(OPT_ZK_SERVERS)) {
                cmdLineConfig.setZkServers(cmdLine.getOptionValue(OPT_ZK_SERVERS));
            }

            configFile = cmdLine.getOptionValue(OPT_CONFIG_FILE, DEFAULT_CONFIG_FILE);

            return true;

        } catch (ParseException exc) {
            LOG.error("Command line error: {}", exc.getMessage());
            usage();
            return false;
        }

    }

    private static void usage() {
        HelpFormatter formatter = new HelpFormatter();
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        PrintWriter pw = new PrintWriter(os);
        try {
            formatter.printHelp(pw, HelpFormatter.DEFAULT_WIDTH, "BKProxy", null, options,
                                HelpFormatter.DEFAULT_LEFT_PAD, HelpFormatter.DEFAULT_DESC_PAD, null);
            pw.flush();
            LOG.error("\n{}", os.toString());
        } finally {
            pw.close();
        }
    }
}

