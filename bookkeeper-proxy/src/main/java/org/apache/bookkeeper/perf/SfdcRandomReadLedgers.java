package org.apache.bookkeeper.perf;

import java.io.File;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.AbstractMap;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.BKExtentId;
import org.apache.bookkeeper.BKExtentIdByteArray;
import org.apache.bookkeeper.BKExtentLedgerMap;
import org.apache.bookkeeper.BKSfdcClient;
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
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.RateLimiter;

public class SfdcRandomReadLedgers {
    static final Logger LOG = LoggerFactory.getLogger(SfdcRandomReadLedgers.class);

    static { //lazy hack
        org.apache.log4j.BasicConfigurator.configure();
        org.apache.log4j.Logger.getRootLogger().setLevel(org.apache.log4j.Level.WARN);
        org.apache.log4j.Logger.getLogger(SfdcRandomReadLedgers.class).setLevel(org.apache.log4j.Level.INFO);
    }

    private static String name(String s) {
        return MetricRegistry.name(SfdcRandomReadLedgers.class, s);
    }

    private static volatile boolean finish = false;
    
    private static final MetricRegistry metrics = new MetricRegistry();
    
    private static final Timer readLatency = metrics.timer(name("readLatency"));
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
            formatter.printHelp(pw, HelpFormatter.DEFAULT_WIDTH, SfdcRandomReadLedgers.class.getName(), null, options,
                                HelpFormatter.DEFAULT_LEFT_PAD, HelpFormatter.DEFAULT_DESC_PAD, null);
            pw.flush();
            LOG.error("\n{}", os.toString());
        } finally {
            pw.close();
        }
    }
    
    // http://stackoverflow.com/questions/2546078/java-random-long-number-in-0-x-n-range
    static long nextLong(Random rng, long n) {
        long bits, val;
        do {
           bits = (rng.nextLong() << 1) >>> 1;
           val = bits % n;
        } while (bits-val+(n-1) < 0L);
        return val;
     }
    
    public static void main(String[] args) throws Exception {
        final Options options = new Options();

        options.addOption("h", "help", false, "print usage message");
        options.addOption("t", "threads", true, "number of threads to use for reading. Default: number of cores.");
        options.addOption("r", "runtime", true, "time to run test, in seconds. Default: 600");
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

        final int runTime = Integer.parseInt(cmdLine.getOptionValue("r", "600"));
        final int numOfThreads = Integer.parseInt(cmdLine.getOptionValue("t", "" + Runtime.getRuntime().availableProcessors()));
        final double maxQps = Double.parseDouble(cmdLine.getOptionValue("q", "1000000"));
        final int warmup = Integer.parseInt(cmdLine.getOptionValue("w", "15"));

        final BookKeeper bk = new BookKeeper(bkConfig);
        final BKExtentLedgerMap elm = BKExtentLedgerMap.newBuilder()
                .setReadHandleCacheLen(bkConfig.getReadHandleCacheLen())
                .setReadHandleTTL(bkConfig.getReadHandleTTL())
                .build();

        final BKSfdcClient bksc = new BKSfdcClient(bkConfig, bk, elm, null);
        
        consoleReporter.start(30, TimeUnit.SECONDS);
        jmxReporter.start();
        
        final List<AbstractMap.SimpleEntry<BKExtentId, Integer>> ledgerInfo = Lists.newArrayList();
        for(long ledger: bksc.ledgerList()) {
            BKExtentId extentId = new BKExtentIdByteArray(ledger);
            int lac;
            try {
                lac = bksc.ledgerLac(extentId);
            } catch(IllegalStateException se) {
                LOG.info("Ledger {} is new and empty, skipping", ledger);
                continue;
            }
            if(lac > 0) {
                AbstractMap.SimpleEntry<BKExtentId, Integer> tuple = 
                        new AbstractMap.SimpleEntry<BKExtentId, Integer>(extentId, lac);
                ledgerInfo.add(tuple);
            }
        }

        final RateLimiter rateLimiter = RateLimiter.create(maxQps, warmup, TimeUnit.SECONDS);
        ExecutorService pool = Executors.newFixedThreadPool(numOfThreads);
        
        for(int i = 0; i < numOfThreads; ++i) {
            pool.execute(new Runnable() {
                @Override
                public void run() {
                    LOG.info("Thread is starting");
                    
                    final BKSfdcClient bkscT = new BKSfdcClient(bkConfig, bk, elm, null);

                    final int numberOfLedgers = ledgerInfo.size();
                    final Random random = new SecureRandom();
                    while(!finish) {
                        final int n = random.nextInt(numberOfLedgers);
                        BKExtentId ledgerId = ledgerInfo.get(n).getKey();
                        int ledgerLac = ledgerInfo.get(n).getValue();
                        
                        int fragmentToRead = random.nextInt(ledgerLac);

                        rateLimiter.acquire();
                        final Timer.Context ct = readLatency.time();
                        try {
                            ByteBuffer data = bkscT.ledgerGetEntry(ledgerId, fragmentToRead, 0);
                            data.flip();
                            bytesRate.mark(data.limit());
                        } catch (Exception e) {
                            finish = true;
                            LOG.error("Exception while reading entry", e);
                        } finally {
                            ct.stop();
                        }
                    }
                }
            });
        }
        
        pool.shutdown();
        try {
          if (!pool.awaitTermination(runTime, TimeUnit.SECONDS)) {
            LOG.info("stopping threads");
            pool.shutdownNow();
            if (!pool.awaitTermination(60, TimeUnit.SECONDS))
                LOG.error("Pool did not terminate");
          }
        } catch (InterruptedException ie) {
          pool.shutdownNow();
          Thread.currentThread().interrupt();
        }
        
        consoleReporter.report();
        
        LOG.info("shutting down reporters and bookie");
        consoleReporter.stop();
        jmxReporter.stop();
        
        bk.close();
    }

}
