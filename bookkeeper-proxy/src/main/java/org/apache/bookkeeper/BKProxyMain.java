package org.apache.bookkeeper;

import static org.apache.bookkeeper.BKProxyStats.PROXY_WORKER_SCOPE;
import static org.apache.bookkeeper.BKProxyStats.WORKER_POOL_COUNT;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.security.AccessControlException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.conf.BookKeeperProxyConfiguration;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stats.StatsProvider;
import org.apache.bookkeeper.util.ReflectionUtils;
import org.apache.bookkeeper.zookeeper.BoundExponentialBackoffRetryPolicy;
import org.apache.bookkeeper.zookeeper.ZooKeeperClient;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
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
    // ThreadpoolExecutor termination await period in millisecs
    private static final long TERMINATION_TIMEOUT_PERIOD = 3000;

    private static String  configFile;
    private static Options options;

    private AtomicBoolean isShutdownExecuted = new AtomicBoolean();
    private AtomicLong bkProxyWorkerNo = new AtomicLong();
    private ServerSocketChannel serverChannel;
    private final BookKeeperProxyConfiguration bkpConf;
    private BookKeeper bk = null;
    private ZooKeeper zkc;
    private ThreadPoolExecutor proxyWorkerExecutor;
    private StatsProvider statsProvider;
    private StatsLogger statsLogger;
    private final Counter proxyWorkerPoolCounter;
    // byteBuffer pool shared by all threads
    private final BKByteBufferPool byteBufSharedPool;
    private int exitCode = ProxyExitCode.OK;

    public class ProxyExitCode {
        // normal quit
        public final static int OK = 0;
        // exception running BookKeeperProxy
        public final static int PROXY_EXCEPTION = 1;
    }

    public BKProxyMain(BookKeeperProxyConfiguration bkpConf) {
        this.bkpConf = bkpConf;
		bkpConf.validateUser();
        proxyWorkerExecutor = new ThreadPoolExecutor(bkpConf.getCorePoolSize(), bkpConf.getWorkerThreadLimit(),
                bkpConf.getCorePoolKeepAliveTime(), TimeUnit.MILLISECONDS, new SynchronousQueue<Runnable>());
        Class<? extends StatsProvider> statsProviderClass = null;
        boolean statsEnabled = bkpConf.getBoolean("enableStatistics", false);
        if (statsEnabled) {
            try {
                statsProviderClass = bkpConf.getStatsProviderClass();
                statsProvider = ReflectionUtils.newInstance(statsProviderClass);
                statsProvider.start(bkpConf);
                statsLogger = statsProvider.getStatsLogger(bkpConf.getString("statsPrefix"));
                System.out.println("Running stats...");
            } catch (ConfigurationException e) {
                statsLogger = NullStatsLogger.INSTANCE;
                LOG.error("Failed to instantiate stats provider class: ", e);
            }
        }
        else {
            // Declare it as null so operations do not log, but also do not throw NPEs
            statsLogger = NullStatsLogger.INSTANCE;
        }
        proxyWorkerPoolCounter = statsLogger.scope(PROXY_WORKER_SCOPE).getCounter(WORKER_POOL_COUNT);

        GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();

        poolConfig.setMaxIdle(bkpConf.getByteBufferPoolSizeMaxIdle());
        LOG.info("Byte Buffer Pool Max Idle: " + poolConfig.getMaxIdle());

        BKByteBufferPoolFactory poolFactory = new BKByteBufferPoolFactory(bkpConf.getByteBufferPoolBufferSize());
        this.byteBufSharedPool = new BKByteBufferPool(poolFactory, poolConfig, statsLogger);
    }

    public BookKeeperProxyConfiguration getBookKeeperProxyConfiguration() {
        return bkpConf;
    }
    
    @Override
    public void run() {
        try {
            serverChannel = ServerSocketChannel.open();
            serverChannel.setOption(java.net.StandardSocketOptions.SO_RCVBUF,
                    bkpConf.getServerChannelReceiveBufferSize());
            String hostname = bkpConf.getBKProxyHostname();
            int port = bkpConf.getBKProxyPort();
            serverChannel.socket().bind(new InetSocketAddress(hostname, port));

            // Global structures
            BKExtentLedgerMap elm = BKExtentLedgerMap.newBuilder()
                    .setReadHandleCacheLen(bkpConf.getReadHandleCacheLen())
                    .setReadHandleTTL(bkpConf.getReadHandleTTL())
                    .build();
            // base time for exponential back-off is set to at least 1sec below; could be higher
            // if conf.getZkTimeout()/10 > 1sec. If the base back-off time happens to be 1sec, the
            // retry back-off times will be chosen randomly between 0 to 2secs the first retry,
            // between 0 to 4sec for the second retry, 0 to 8secs for the third and so on.
            this.zkc = ZooKeeperClient.newBuilder().connectString(bkpConf.getZkServers())
                    .sessionTimeoutMs(bkpConf.getZkTimeout())
                    .operationRetryPolicy(
                            new BoundExponentialBackoffRetryPolicy(Math.max(1000, bkpConf.getZkTimeout() / 10),
                                    bkpConf.getZkTimeout(), bkpConf.getZkOpRetryCount()))
                    .statsLogger(statsLogger).build();
            if (statsProvider != null) {
                bk = BookKeeper.forConfig(bkpConf).setStatsLogger(statsLogger).setZookeeper(zkc).build();
            } else {
                bk = BookKeeper.forConfig(bkpConf).setZookeeper(zkc).build();
            }

            while (!proxyWorkerExecutor.isShutdown()) {
                LOG.info("SFStore BK-Proxy: Waiting for connection... ");
                // Let jenkins know that we are up.
                System.out.println("SFStore BK-Proxy: Waiting for connection... ");

                SocketChannel sock = null;
                try {
                    try {
                        sock = serverChannel.accept();
                    } catch (ClosedChannelException cce) {
                        // we can get this exception when serverChannel.close() is called
                        LOG.error("Channel has been closed, so terminating main thread. Exception:", cce);
                        exitCode = ProxyExitCode.PROXY_EXCEPTION;
                        break;
                    }
                    String remoteAddress = "";
                    try {
                        remoteAddress = sock.getRemoteAddress().toString();
                        LOG.debug("Accepted connection from: {}", remoteAddress);
                    } catch (Exception e) {
                        LOG.error("Exception trying to get client address", e);
                    }

                    try {
                        BKProxyWorker bkProxyWorker = BKProxyWorker.newBuilder().setBKPConfig(bkpConf)
                                .setClientSocketChannel(sock).setBookKeeperClient(bk).setZooKeeperClient(zkc)
                                .setLedgerMap(elm).setByteBufPool(byteBufSharedPool)
                                .setBKProxyWorkerId(bkProxyWorkerNo.getAndIncrement())
                                .setStatsLogger(statsLogger.scope(PROXY_WORKER_SCOPE)).build();

                        proxyWorkerExecutor.submit(bkProxyWorker);
                        proxyWorkerPoolCounter.inc();
                    } catch (RejectedExecutionException rejException) {
                        LOG.error("ProxyWorkerExecutor has rejected new task. Current number of active Threads: "
                                + proxyWorkerExecutor.getActiveCount() + " RemoteAddress: " + remoteAddress,
                                rejException);
                        continue;
                    }
                    sock = null;
                } finally {
                    if (sock != null) {
                        sock.close();
                        sock = null;
                    }
                }
            }
        } catch (IOException | InterruptedException | KeeperException e) {
            LOG.error("Exception in starting a new bookkeeper proxy thread", e);
            exitCode = ProxyExitCode.PROXY_EXCEPTION;
            return;
        } catch (Exception e) {
            LOG.error("Exception in starting a new bookkeeper proxy thread", e);
            exitCode = ProxyExitCode.PROXY_EXCEPTION;
            throw e;
        }
    }

    public void shutdown() {
        if (isShutdownExecuted.compareAndSet(false, true)) {
            LOG.info("Shutting down ProxyWorkerExecutor...");
            proxyWorkerExecutor.shutdownNow();
            try {
                LOG.info("Awaiting termination of BKProxyWorkers...");
                if (!proxyWorkerExecutor.awaitTermination(TERMINATION_TIMEOUT_PERIOD, TimeUnit.MILLISECONDS)) {
                    LOG.warn("All BKProxyWorkerThreads are not terminated even after " + TERMINATION_TIMEOUT_PERIOD
                            + "milliSecs. But still we are proceeding with shutdown process");
                }
            } catch (InterruptedException e) {
                LOG.warn("Got interrupted while waiting for termination of ProxyWorkers", e);
            }
            if (byteBufSharedPool != null) {
                byteBufSharedPool.close();
            }
            if (statsProvider != null) {
                LOG.info("Stats provider stopped...");
                statsProvider.stop();
            }
            if (serverChannel != null) {
                LOG.info("Closing ServerSocketChannel...");
                try {
                    serverChannel.close();
                } catch (IOException e) {
                    LOG.warn("Got IOException while closing the ServerSocketChannel", e);
                }
            }
            if (bk != null) {
                LOG.info("Closing BookKeeper...");
                try {
                    bk.close();
                } catch (InterruptedException | BKException e) {
                    LOG.warn("Got Exception while closing the BK Client", e);
                }
            }
            if(zkc != null){
                LOG.info("Closing ZooKeeper Client...");
                try {
                    zkc.close();
                } catch (InterruptedException e) {
                    LOG.warn("Got InterruptedException while closing Zookeeper Client", e);
                }
            }
        }
    }

    public static void main(String args[]) throws Exception {
        BookKeeperProxyConfiguration bkConfig = new BookKeeperProxyConfiguration();
        final AtomicReference<BKProxyMain> bkMainReference = new AtomicReference<BKProxyMain>();
        try {
            BookKeeperProxyConfiguration cmdLineConfig = new BookKeeperProxyConfiguration();
            BookKeeperProxyConfiguration fileConfig = new BookKeeperProxyConfiguration();

            if (parseArgs(args, cmdLineConfig)) {
                loadConfFile(fileConfig, configFile);

                // Compose the configuration files such that cmd line has precedence over the config file
                bkConfig.addConfiguration(cmdLineConfig);
                bkConfig.addConfiguration(fileConfig);

                bkMainReference.set(new BKProxyMain(bkConfig));
                Thread bkWorkerThreadController = new Thread(bkMainReference.get(), "BKProxyMain");
                bkWorkerThreadController.start();
                Runtime.getRuntime().addShutdownHook(new Thread() {
                    @Override
                    public void run() {
                        LOG.info("ShutdownHook is triggered");
                        bkMainReference.get().shutdown();
                        LOG.info("ShutdownHook of BKProxyMain ran successfully");
                    }
                });
                LOG.info("Registered the shutdown hook successfully");
                bkWorkerThreadController.join();
                LOG.info("BKProxyMain instance thread has died");
            }
        } catch (Exception e) {
            LOG.error("Exception in main thread:", e);
            throw e;
        } finally {
            if (bkMainReference.get() != null) {
                LOG.info("Shutting down BKProxy Instance before exiting");
                bkMainReference.get().shutdown();
                LOG.info("Exiting with exitcode: " + bkMainReference.get().exitCode);
                System.exit(bkMainReference.get().exitCode);
            }
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
                cmdLineConfig.setBKProxyPort(Integer.parseInt(cmdLine.getOptionValue(OPT_PORT)));
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
            System.out.println(os.toString());
        } finally {
            pw.close();
        }
    }
}

