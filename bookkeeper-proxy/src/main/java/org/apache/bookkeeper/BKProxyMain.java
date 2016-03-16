package org.apache.bookkeeper;

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
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicInteger;

/*
 * BKProxyMain implements runnable method and when Thread is spawned it starts ServerSocketChannel on the provided bkProxyPort.
 * Clients connect to bkProxyPort and the client channel is created on accept()
 * A new worker thread is generated for each client channel and that thread ends when client closes the connection
 *
 * The main() method starts BKProxyMain Thread on the well known port (5555) and waits for its completion.
 */
public class BKProxyMain implements Runnable {

    private static final String DEFAULT_CONFIG_FILE = "conf/bk_client_proxy.conf";

    private static final String OPT_HELP_FLAG        = "h";
    private static final String OPT_ZK_SERVERS       = "z";
    private static final String OPT_CONFIG_FILE      = "c";

    private static String  configFile;
    private static Options options;

    private AtomicInteger threadNum = new AtomicInteger();
    private final static Logger LOG = LoggerFactory.getLogger(BKProxyMain.class);

    private ServerSocketChannel serverChannel;
    private final BookKeeperProxyConfiguration bkpConf;
    private BookKeeper bk = null;

    public BKProxyMain(BookKeeperProxyConfiguration bkpConf) {
        this.bkpConf = bkpConf;
    }

    public BookKeeperProxyConfiguration getBookKeeperProxyConfiguration() {
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
        BookKeeperProxyConfiguration cmdLineConfig = new BookKeeperProxyConfiguration();
        BookKeeperProxyConfiguration fileConfig = new BookKeeperProxyConfiguration();

        if (parseArgs(args, cmdLineConfig)) {
            loadConfFile(fileConfig, configFile);

            // Compose the configuration files such that cmd line has precedence over the config file
            bkConfig.addConfiguration(cmdLineConfig);
            bkConfig.addConfiguration(fileConfig);

            BKProxyMain bkMain = new BKProxyMain(bkConfig);
            Thread bkMainThread = new Thread(bkMain);
            bkMainThread.start();
            bkMainThread.join();
        }
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


    @SuppressWarnings("static-access")
    private static boolean parseArgs(String[] args, BookKeeperProxyConfiguration cmdLineConfig) {
        options = new Options();

        options.addOption(OPT_HELP_FLAG, "help", false, "print usage message");
        options.addOption(OPT_ZK_SERVERS, "zk-servers", true, "list of Zookeeper servers");
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

    static void usage() {
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

