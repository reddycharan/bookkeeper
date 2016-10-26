package org.apache.bookkeeper.perf;

import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.security.SecureRandom;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.bookkeeper.util.NativeIO;
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

public class JournalSimulator {
    static final Logger LOG = LoggerFactory.getLogger(JournalSimulator.class);

    private static String name(String s) {
        return MetricRegistry.name(JournalSimulator.class, s);
    }

    private static final MetricRegistry metrics = new MetricRegistry();

    private static final Timer writeLatency = metrics.timer(name("writeLatency"));
    private static final Timer totalWriteLatency = metrics.timer(name("totalWriteLatency"));
    private static final Timer preallocLatency = metrics.timer(name("preallocLatency"));
    private static final Timer forceWriteLatency = metrics.timer(name("forceWriteLatency"));
    private static final Timer bestEffortRemoveFromPageCacheLatency = metrics
            .timer(name("bestEffortRemoveFromPageCacheLatency"));

    private static final Meter bytesRate = metrics.meter(name("bytesRate"));
    private static final Meter preallocRate = metrics.meter(name("preallocRate"));

    private static final ConsoleReporter consoleReporter = ConsoleReporter.forRegistry(metrics)
            .convertRatesTo(TimeUnit.SECONDS).convertDurationsTo(TimeUnit.MICROSECONDS).build();
    private static final JmxReporter jmxReporter = JmxReporter.forRegistry(metrics).convertRatesTo(TimeUnit.SECONDS)
            .convertDurationsTo(TimeUnit.MICROSECONDS).build();

    private static void usage(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        PrintWriter pw = new PrintWriter(os);
        try {
            formatter.printHelp(pw, HelpFormatter.DEFAULT_WIDTH, SfdcGenerateLedger.class.getName(), null, options,
                    HelpFormatter.DEFAULT_LEFT_PAD, HelpFormatter.DEFAULT_DESC_PAD, null);
            pw.flush();
            LOG.error("\n{}", os.toString());
            System.out.println(os.toString());
        } finally {
            pw.close();
        }
    }

    public static void main(String[] args) throws Exception {

        final Options options = new Options();

        options.addOption("h", "help", false, "print usage message");
        options.addOption("d", "directory", true, "directory where the files will be created");
        options.addOption("p", "file-prefix", true, "prefix for the file name");
        options.addOption("e", "entry-size", true, "size of the entry in bytes. default: 65536");
        options.addOption("a", "prealloc-size", true, "journal prealloc size in Mb; default: 4");
        options.addOption("b", "align-size", true, "journal align size in bytes; default: 512");
        options.addOption("m", "max-files", true, "files to create. default: 250");
        options.addOption("z", "file-size", true, "max size of the file in Gb. default: 1");
        options.addOption("f", "force-write", true, "force write of each entry. default: true");
        options.addOption("c", "remove-page-cache", true,
                "call bestEffortRemoveFromPageCache after each entry force-write. default: true");
        options.addOption("t", "threads", true, "number of threads to use for writing. Default: 1.");

        CommandLine cmdLine;
        try {
            CommandLineParser parser = new GnuParser();
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

        final String directory = cmdLine.getOptionValue("d");
        final String filePrefix = cmdLine.getOptionValue("p");

        final int ENTRY_SIZE = Integer.parseInt(cmdLine.getOptionValue("e", "65536"));
        final byte[] data = new byte[ENTRY_SIZE];
        (new SecureRandom()).nextBytes(data);

        final long preAllocSize = 1024L * 1024 * Long.parseLong(cmdLine.getOptionValue("a", "4"));
        final int journalAlignSize = Integer.parseInt(cmdLine.getOptionValue("b", "512"));

        final int MAX_FILES = Integer.parseInt(cmdLine.getOptionValue("m", "250"));
        final long maxSize = 1024L * 1024 * 1024 * Long.parseLong(cmdLine.getOptionValue("z", "1"));
        final boolean doForceWrite = Boolean.parseBoolean(cmdLine.getOptionValue("f", "true"));
        final boolean doPageCacheRemove = Boolean.parseBoolean(cmdLine.getOptionValue("c", "true"));

        final int numOfThreads = Integer.parseInt(cmdLine.getOptionValue("t", "1"));
        ExecutorService pool = Executors.newFixedThreadPool(numOfThreads);

        cleanUp(directory, filePrefix, MAX_FILES);

        final Queue<Closeable> toClose = new ConcurrentLinkedQueue<>();

        consoleReporter.start(30, TimeUnit.SECONDS);
        jmxReporter.start();

        final AtomicInteger current = new AtomicInteger(0);
        for (int i = 0; i < numOfThreads; ++i) {
            pool.execute(new Runnable() {
                @Override
                public void run() {
                    final ByteBuffer bb = ByteBuffer.wrap(data);
                    final ByteBuffer zeros = ByteBuffer.allocate(journalAlignSize);

                    while (true) {
                        int num = current.getAndIncrement();
                        if (num >= MAX_FILES) {
                            return;
                        }

                        try {
                            long nextPrealloc = 0;

                            File f = new File(directory, filePrefix + num);
                            FileOutputStream fos = new FileOutputStream(f);
                            int fd = NativeIO.getSysFileDescriptor(fos.getFD());
                            FileChannel fc = fos.getChannel();
                            toClose.add(fc);
                            toClose.add(fos);

                            long pos = 0L;
                            while (pos < maxSize) {

                                if (pos + data.length < nextPrealloc) {
                                    bb.rewind();

                                    final Timer.Context totalCt = totalWriteLatency.time();

                                    final Timer.Context wCt = writeLatency.time();
                                    fc.write(bb);
                                    wCt.stop();

                                    bytesRate.mark(data.length);
                                    pos += data.length;

                                    if (doForceWrite) {
                                        final Timer.Context flCt = forceWriteLatency.time();
                                        fc.force(false);
                                        flCt.stop();

                                        if (doPageCacheRemove) {
                                            bestEffortRemoveFromPageCache(fd, pos);
                                        }
                                    }

                                    totalCt.stop();
                                } else {
                                    nextPrealloc += preAllocSize;

                                    final Timer.Context paCt = preallocLatency.time();
                                    fc.write(zeros, nextPrealloc - journalAlignSize);
                                    paCt.stop();

                                    preallocRate.mark(preAllocSize);
                                    bytesRate.mark(journalAlignSize);
                                    zeros.clear();
                                }
                            }

                            final Timer.Context flCt = forceWriteLatency.time();
                            fc.force(true);
                            flCt.stop();
                        } catch (Exception e) {
                            LOG.error("unexpected exception, stopping thread", e);
                        }
                    }
                }
            });
        }


        pool.shutdown();
        while (!pool.awaitTermination(5, TimeUnit.SECONDS)) {
            // noop
        }

        LOG.info("Closing files");
        for (Closeable c : toClose) {
            c.close();
        }
        cleanUp(directory, filePrefix, MAX_FILES);

        consoleReporter.report();
        consoleReporter.stop();
        jmxReporter.stop();
    }

    private static long CACHE_DROP_LAG_BYTES = 8 * 1024 * 1024;
    private static long lastDropPosition = 0L;

    static public void bestEffortRemoveFromPageCache(int fd, long newForceWritePosition) throws IOException {
        long newDropPos = newForceWritePosition - CACHE_DROP_LAG_BYTES;
        if (lastDropPosition < newDropPos) {
            final Timer.Context ct = bestEffortRemoveFromPageCacheLatency.time();
            NativeIO.bestEffortRemoveFromPageCache(fd, lastDropPosition, newDropPos - lastDropPosition);
            ct.stop();
        }
        lastDropPosition = newDropPos;
    }

    private static void cleanUp(final String directory, final String prefix, final int MAX_FILES) {
        for (int num = 0; num < MAX_FILES; num++) {
            String fileName = prefix + num;
            File f = new File(directory, fileName);
            if (f.exists()) {
                f.delete();
            }
        }
    }
}
