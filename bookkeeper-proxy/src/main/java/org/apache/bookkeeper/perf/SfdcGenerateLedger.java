package org.apache.bookkeeper.perf;

import java.io.File;
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.bookkeeper.BKExtentId;
import org.apache.bookkeeper.BKExtentLedgerMap;
import org.apache.bookkeeper.BKSfdcClient;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.conf.BookKeeperProxyConfiguration;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.util.concurrent.RateLimiter;

public class SfdcGenerateLedger {
    static final Logger LOG = LoggerFactory.getLogger(SfdcGenerateLedger.class);

    final static int ENTRY_SIZE = 64 * 1024;
    final static byte[] data64k = new byte[ENTRY_SIZE];
    static {
        (new SecureRandom()).nextBytes(data64k);
    }

    /*
    static { //lazy hack
        org.apache.log4j.BasicConfigurator.configure();
        org.apache.log4j.Logger.getRootLogger().setLevel(org.apache.log4j.Level.WARN);
        org.apache.log4j.Logger.getLogger(SfdcGenerateLedger.class).setLevel(org.apache.log4j.Level.INFO);
    }
    */

    private static String name(String s) {
        return MetricRegistry.name(SfdcGenerateLedger.class, s);
    }

    private static volatile boolean finish = false;
    
    private static final MetricRegistry metrics = new MetricRegistry();
    
    private static final Timer writeLatency = metrics.timer(name("writeLatency"));
    private static final Meter bytesRate = metrics.meter(name("bytesRate"));

    private static final ConsoleReporter consoleReporter = ConsoleReporter.forRegistry(metrics)
            .convertRatesTo(TimeUnit.SECONDS)
            .convertDurationsTo(TimeUnit.MICROSECONDS)
            .build();
    private static final JmxReporter jmxReporter = JmxReporter.forRegistry(metrics)
            .convertRatesTo(TimeUnit.SECONDS)
            .convertDurationsTo(TimeUnit.MICROSECONDS)
            .build();

    
    private static void usage(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        PrintWriter pw = new PrintWriter(os);
        try {
            formatter.printHelp(pw, HelpFormatter.DEFAULT_WIDTH, SfdcGenerateLedger.class.getName(), null, options,
                                HelpFormatter.DEFAULT_LEFT_PAD, HelpFormatter.DEFAULT_DESC_PAD, null);
            pw.flush();
            LOG.error("\n{}", os.toString());
        } finally {
            pw.close();
        }
    }
    
    private static String generateThreadDump() {
        final StringBuilder dump = new StringBuilder();
        dump.append("\n");
        final ThreadMXBean mxBean = ManagementFactory.getThreadMXBean();
        final ThreadInfo[] infos = mxBean.getThreadInfo(mxBean.getAllThreadIds(), 100);
        
        long[] deadlockedIds = mxBean.findDeadlockedThreads(); // Returns null if no threads are deadlocked.
        if (deadlockedIds != null) {
            ThreadInfo[] deadlockedInfos = mxBean.getThreadInfo(deadlockedIds, 100);

            dump.append("DEADLOCK DETECTED: \n\n");
            for (ThreadInfo di : deadlockedInfos) {
                dump.append(di.toString()).append("\n");
            }
            dump.append("\n\n/DEADLOCK\n");
        }

        dump.append("ALL threads: \n\n");
        
        for(ThreadInfo ti: infos) {
            dump.append(ti.toString()).append("\n");
        }
        return dump.toString();
    }
    
    public static void main(String[] args) throws Exception {
        final Options options = new Options();

        options.addOption("h", "help", false, "print usage message");
        options.addOption("l", "ledgers", true, "number of ledgers per thread to generate. Default: 25");
        options.addOption("s", "size", true, "total size of ledgers in megabytes. Default: 2048");
        options.addOption("t", "threads", true, "number of threads to use for writing. Default: number of cores.");
        options.addOption("q", "qps", true, "max QPS. Default: 1,000,000. Warmup period is 1 min");
        options.addOption("w", "warmup", true, "Warmup period in seconds. Default: 15 sec");
        options.addOption("c", "config-file", true, "path to configuration file. Default: conf/bk_client_proxy.conf");

        CommandLine cmdLine;
        try {
            CommandLineParser parser  = new GnuParser();
            cmdLine = parser.parse(options, args);

            if (cmdLine.hasOption("h")) {
                usage(options);
                return;
            }
        } catch (ParseException exc) {
            LOG.error("Command line error: {}", exc.getMessage());
            usage(options);
            return;
        }

        final BookKeeperProxyConfiguration bkConfig = new BookKeeperProxyConfiguration();
        final String configFile = cmdLine.getOptionValue("c", "conf/bk_client_proxy.conf");
        bkConfig.loadConf(new File(configFile).toURI().toURL());

        final int numberOfLedgers = Integer.parseInt(cmdLine.getOptionValue("l", "25"));
        final long maxSizeBytes = Long.parseLong(cmdLine.getOptionValue("s", "2048")) * 1024 * 1024;
        final long maxEntries = maxSizeBytes / ENTRY_SIZE;
        final int numOfThreads = Integer.parseInt(cmdLine.getOptionValue("t", "" + Runtime.getRuntime().availableProcessors()));
        final double maxQps = Double.parseDouble(cmdLine.getOptionValue("q", "1000000"));
        final int warmup = Integer.parseInt(cmdLine.getOptionValue("w", "15"));

        final BookKeeper bk = new BookKeeper(bkConfig);
        final BKExtentLedgerMap elm = BKExtentLedgerMap.newBuilder()
                .setReadHandleCacheLen(bkConfig.getReadHandleCacheLen())
                .setReadHandleTTL(bkConfig.getReadHandleTTL())
                .build();

        consoleReporter.start(30, TimeUnit.SECONDS);
        jmxReporter.start();
        
        final AtomicLong entriesToAdd = new AtomicLong(maxEntries);
        final AtomicBoolean doTheDump = new AtomicBoolean(true);
        
        final RateLimiter rateLimiter = RateLimiter.create(maxQps, warmup, TimeUnit.SECONDS);
        ExecutorService pool = Executors.newFixedThreadPool(numOfThreads);
        
        for(int i = 0; i < numOfThreads; ++i) {
            pool.execute(new Runnable() {
                @Override
                public void run() {
                    LOG.info("Thread is starting");
                    final Random random = new SecureRandom();
                    
                    final BKSfdcClient bkscT = new BKSfdcClient(bkConfig, bk, elm, null, null);
                    
                    final BKExtentId[] extIds = new BKExtentId[numberOfLedgers];
                    for(int i = 0; i < numberOfLedgers; ++i) {
                        try {
                            extIds[i] = bkscT.ledgerCreate();
                        } catch (BKException | InterruptedException e) {
                            LOG.error("failed to create a ledger", e);
                            LOG.info("Thread finished");
                            return;
                        }
                        LOG.info("Created ledger with id {}", extIds[i].asLong());
                    }
                    
                    final int[] fragmentIds = new int[numberOfLedgers];
                    for(int i = 0; i< numberOfLedgers; ++i) {
                        fragmentIds[i] = 0;
                    }
                    
                    while(entriesToAdd.decrementAndGet() > 0L && !finish) {
                        final int ledgerNum = random.nextInt(numberOfLedgers);
                        ++fragmentIds[ledgerNum];

                        final ByteBuffer bdata = ByteBuffer.wrap(data64k);
                        
                        rateLimiter.acquire();
                        final Timer.Context ct = writeLatency.time();
                        try {
                            bkscT.ledgerPutEntry(extIds[ledgerNum], fragmentIds[ledgerNum], bdata, null);
                            bytesRate.mark(data64k.length);
                        } catch (Exception e) {
                            finish = true;
                            try {
                                LOG.info("Client got error while writing: L{} E{} LAC = " + (fragmentIds[ledgerNum] > 10 ? bkscT.ledgerLac(extIds[ledgerNum]) : -1), 
                                        extIds[ledgerNum], fragmentIds[ledgerNum] - 1);
                            } catch (BKException | InterruptedException e1) {
                                LOG.error("unexpected error while logging entry", e);
                            }
                            LOG.error("unexpected error while writing entry", e);
                            if(doTheDump.compareAndSet(true, false)) {
                                LOG.warn("Got thread dump: {}", generateThreadDump());
                            }
                        } finally {
                            ct.stop();
                        }
                    }
                    
                    for(int i = 0; i < numberOfLedgers; ++i) {
                        LOG.info("closing ledger {}", extIds[i].asLong());
                        try {
                            bkscT.ledgerWriteClose(extIds[i]);
                        } catch (BKException e) {
                            LOG.error("unexpected error while closing ledger", e);
                        } catch (InterruptedException e) {
                            LOG.error("unexpected error while closing ledger", e);
                            Thread.currentThread().interrupt();
                        }
                    }
                    LOG.info("Thread finished");
                }
            });
        }
        
        pool.shutdown();
        while(!pool.awaitTermination(15, TimeUnit.SECONDS));
        consoleReporter.report();
        
        LOG.info("shutting down reporters and bookie");
        consoleReporter.stop();
        jmxReporter.stop();
        
        bk.close();
    }

}
