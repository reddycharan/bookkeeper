package org.apache.bookkeeper.perf;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.security.SecureRandom;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

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

public class SfdcRandomReadRaw {
    static final Logger LOG = LoggerFactory.getLogger(SfdcRandomReadRaw.class);

    final static int ENTRY_SIZE = 64 * 1024;
    
    static { //lazy hack
        org.apache.log4j.BasicConfigurator.configure();
        org.apache.log4j.Logger.getRootLogger().setLevel(org.apache.log4j.Level.WARN);
        org.apache.log4j.Logger.getLogger(SfdcRandomReadRaw.class).setLevel(org.apache.log4j.Level.INFO);
    }

    private static String name(String s) {
        return MetricRegistry.name(SfdcRandomReadRaw.class, s);
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
            formatter.printHelp(pw, HelpFormatter.DEFAULT_WIDTH, SfdcRandomReadRaw.class.getName(), null, options,
                                HelpFormatter.DEFAULT_LEFT_PAD, HelpFormatter.DEFAULT_DESC_PAD, null);
            pw.flush();
            LOG.error("\n{}", os.toString());
        } finally {
            pw.close();
        }
    }
    
    private static int readFc(FileChannel fc, ByteBuffer bb, long start) throws IOException {
        int total = 0;
        int rc = 0;
        while(bb.remaining() > 0) {
            rc = fc.read(bb, start);
            if (rc <= 0) {
                throw new IOException("Short read");
            }
            total += rc;
            // should move read position
            start += rc;
        }
        return total;
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
        options.addOption("d", "directory", true, "path to directory with *.* files (required)");

        CommandLine cmdLine;
        try {
            CommandLineParser parser  = new GnuParser();
            cmdLine = parser.parse(options, args);

            if (cmdLine.hasOption("h") || ! cmdLine.hasOption("d")) {
                usage(options);
                return;
            }
        } catch (ParseException exc) {
            LOG.error("Command line error: {}", exc.getMessage());
            usage(options);
            return;
        }

        final File[] dataFiles;
        try { 
            final File dataDir = new File(cmdLine.getOptionValue("d"));
            dataFiles = dataDir.listFiles(new FilenameFilter() {
                @Override
                public boolean accept(File dir, String name) {
                    return name.contains(".");
                }
            });
        } catch(Throwable t) {
            LOG.error("Problem finding data files in {}" , cmdLine.getOptionValue("d"), t);
            return;
        }

        if(dataFiles == null || dataFiles.length == 0) {
            LOG.error("Problem finding data files in {}" , cmdLine.getOptionValue("d"));
            return;
        }
        
        final int runTime = Integer.parseInt(cmdLine.getOptionValue("r", "600"));
        final int numOfThreads = Integer.parseInt(cmdLine.getOptionValue("t", "" + Runtime.getRuntime().availableProcessors()));
        final double maxQps = Double.parseDouble(cmdLine.getOptionValue("q", "1000000"));
        final int warmup = Integer.parseInt(cmdLine.getOptionValue("w", "15"));

        if(numOfThreads > dataFiles.length) {
            LOG.error("not enough files {} for requested thread count {}", dataFiles.length, numOfThreads);
            return;
        }
        
        consoleReporter.start(30, TimeUnit.SECONDS);
        jmxReporter.start();
        
        final RateLimiter rateLimiter = RateLimiter.create(maxQps, warmup, TimeUnit.SECONDS);
        ExecutorService pool = Executors.newFixedThreadPool(numOfThreads);

        final AtomicInteger threadId = new AtomicInteger(0);
        for(int i = 0; i < numOfThreads; ++i) {
            pool.execute(new Runnable() {
                @SuppressWarnings("resource")
                @Override
                public void run() {
                    LOG.info("Thread is starting");
                    final int myId = threadId.getAndIncrement();
                    
                    final FileChannel[] fileInfos = new FileChannel[dataFiles.length / numOfThreads];
                    final long[] numEntries = new long[fileInfos.length];
                    
                    final byte[] dataArr = new byte[ENTRY_SIZE];
                    final ByteBuffer data = ByteBuffer.wrap(dataArr);
                    
                    for(int i = myId, idx = 0; 
                            i < dataFiles.length && idx < fileInfos.length; 
                            i += numOfThreads, idx++) {
                        final FileChannel fc;
                        try {
                            fc = new RandomAccessFile(dataFiles[i], "r").getChannel();
                        } catch (FileNotFoundException e) {
                            LOG.error("Failed to open file", e);
                            return;
                        }
                        numEntries[idx] = dataFiles[i].length() / ENTRY_SIZE; 
                        fileInfos[idx] = fc;
                    }
                    
                    final int numberOfFiles = fileInfos.length;
                    final Random random = new SecureRandom();
                    while(!finish) {
                        final int n = random.nextInt(numberOfFiles);
                        final FileChannel fc = fileInfos[n];
                        final long entryToRead = nextLong(random, numEntries[n]);

                        rateLimiter.acquire();
                        final Timer.Context ct = readLatency.time();
                        try {
                            data.rewind();
                            readFc(fc, data, entryToRead * ENTRY_SIZE);
                            bytesRate.mark(data.limit());
                        } catch (Exception e) {
                            finish = true;
                            LOG.error("Exception while reading entry", e);
                        } finally {
                            ct.stop();
                        }
                    }
                    
                    for(FileChannel fc: fileInfos) {
                        try {
                            fc.close();
                        } catch (IOException e) {
                            LOG.error("error closing file channel", e);
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
    }

}
