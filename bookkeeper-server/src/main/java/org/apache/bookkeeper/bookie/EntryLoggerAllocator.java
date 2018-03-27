/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.bookkeeper.bookie;

import static com.google.common.base.Charsets.UTF_8;
import static org.apache.bookkeeper.bookie.TransactionalEntryLogCompactor.COMPACTING_SUFFIX;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.bookkeeper.bookie.LedgerDirsManager.NoWritableLedgerDirException;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An allocator pre-allocates entry log files.
 */
class EntryLoggerAllocator {
    private static final Logger LOG = LoggerFactory.getLogger(EntryLoggerAllocator.class);

    final ByteBuf logfileHeader = Unpooled.buffer(EntryLogger.LOGFILE_HEADER_SIZE);

    private long preallocatedLogId;
    Future<EntryLogger.BufferedLogChannel> preallocation = null;
    private ExecutorService allocatorExecutor;
    private final boolean entryLogPreAllocationEnabled;
    private final ServerConfiguration conf;
    private final LedgerDirsManager ledgerDirsManager;
    private final EntryLogger.EntryLogManager entryLogManager;
    private final EntryLogger.RecentEntryLogsStatus recentlyCreatedEntryLogsStatus;

    EntryLoggerAllocator(ServerConfiguration conf, LedgerDirsManager ledgerDirsManager,
                         EntryLogger.EntryLogManager entryLogManager,
                         EntryLogger.RecentEntryLogsStatus recentlyCreatedEntryLogsStatus,
                         long logId) {
        this.conf = conf;
        this.ledgerDirsManager = ledgerDirsManager;
        this.entryLogManager = entryLogManager;
        this.recentlyCreatedEntryLogsStatus = recentlyCreatedEntryLogsStatus;

        // Initialize the entry log header buffer. This cannot be a static object
        // since in our unit tests, we run multiple Bookies and thus EntryLoggers
        // within the same JVM. All of these Bookie instances access this header
        // so there can be race conditions when entry logs are rolled over and
        // this header buffer is cleared before writing it into the new logChannel.
        logfileHeader.writeBytes("BKLO".getBytes(UTF_8));
        logfileHeader.writeInt(EntryLogger.HEADER_CURRENT_VERSION);
        logfileHeader.writerIndex(EntryLogger.LOGFILE_HEADER_SIZE);

        preallocatedLogId = logId;
        allocatorExecutor = Executors.newSingleThreadExecutor();
        this.entryLogPreAllocationEnabled = conf.isEntryLogFilePreAllocationEnabled();
    }

    synchronized long getPreallocatedLogId(){
        return preallocatedLogId;
    }

    synchronized EntryLogger.BufferedLogChannel createNewLog() throws IOException {
        EntryLogger.BufferedLogChannel bc;
        if (!entryLogPreAllocationEnabled || null == preallocation) {
            // initialization time to create a new log
            bc = allocateNewLog();
        } else {
            // has a preallocated entry log
            try {
                /*
                 * both createNewLog and allocateNewLog are synchronized on
                 * EntryLoggerAllocator.this object. So it is possible that
                 * a thread calling createNewLog would attain the lock on
                 * this object and get to this point but preallocation
                 * Future is starving for lock on EntryLoggerAllocator.this
                 * to execute allocateNewLog. Here since we attained lock
                 * for this it means preallocation future must have either
                 * completed creating new log or still waiting for lock on
                 * this object to execute allocateNewLog method. So we
                 * should try to get result of the future without waiting.
                 * If it fails with TimeoutException then call
                 * allocateNewLog explicitly since we are holding the lock
                 * on this anyhow.
                 *
                 */
                bc = preallocation.get(0, TimeUnit.MILLISECONDS);
            } catch (ExecutionException ee) {
                if (ee.getCause() instanceof IOException) {
                    throw (IOException) (ee.getCause());
                } else {
                    throw new IOException("Error to execute entry log allocation.", ee);
                }
            } catch (CancellationException ce) {
                throw new IOException("Task to allocate a new entry log is cancelled.", ce);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new IOException("Intrrupted when waiting a new entry log to be allocated.", ie);
            } catch (TimeoutException e) {
                LOG.debug("Received TimeoutException while trying to get preallocation future result,"
                          + " which means that Future is waiting for acquiring lock on EntryLoggerAllocator.this");
                bc = allocateNewLog();
            }
        }
        if (entryLogPreAllocationEnabled) {
            /*
             * We should submit new callable / create new instance of future only if the previous preallocation is
             * null or if it is done. This is needed because previous preallocation has not completed its execution
             * since it is waiting for lock on EntryLoggerAllocator.this.
             */
            if ((preallocation == null) || preallocation.isDone()) {
                preallocation = allocatorExecutor.submit(new Callable<EntryLogger.BufferedLogChannel>() {
                        @Override
                        public EntryLogger.BufferedLogChannel call() throws IOException {
                            return allocateNewLog();
                        }
                    });
            }
        }
        LOG.info("Created new entry logger {}.", bc.getLogId());
        return bc;
    }

    synchronized EntryLogger.BufferedLogChannel createNewLogForCompaction() throws IOException {
        return allocateNewLog(COMPACTING_SUFFIX);
    }

    synchronized EntryLogger.BufferedLogChannel allocateNewLog() throws IOException {
        return allocateNewLog(EntryLogger.LOG_FILE_SUFFIX);
    }

    /**
     * Allocate a new log file.
     */
    synchronized EntryLogger.BufferedLogChannel allocateNewLog(String suffix) throws IOException {
        File dirForNextEntryLog;
        List<File> list;

        try {
            list = ledgerDirsManager.getWritableLedgerDirs();
        } catch (NoWritableLedgerDirException nwe) {
            if (!ledgerDirsManager.hasWritableLedgerDirs()) {
                list = ledgerDirsManager.getWritableLedgerDirsForNewLog();
            } else {
                LOG.error("All Disks are not full, but getWritableLedgerDirs threw exception ", nwe);
                throw nwe;
            }
        }

        dirForNextEntryLog = entryLogManager.getDirForNextEntryLog(list);

        List<File> ledgersDirs = ledgerDirsManager.getAllLedgerDirs();
        String logFileName;
        while (true) {
            if (preallocatedLogId >= Integer.MAX_VALUE) {
                preallocatedLogId = 0;
            } else {
                ++preallocatedLogId;
            }
            /*
             * make sure there is no entrylog which already has the same
             * logID. Have to check all the ledegerdirs. If already there is
             * an entrylog with that logid then move to next ID.
             */
            String preallocatedLogIdHexString = Long.toHexString(preallocatedLogId);
            logFileName = preallocatedLogIdHexString + suffix;
            boolean entryLogAlreadyExistsWithThisId = false;
            for (File dir : ledgersDirs) {
                File[] entrylogFilesWithPreallocatedLogId = dir.listFiles((file) -> {
                        if (file.getName().startsWith(preallocatedLogIdHexString + ".")) {
                            LOG.warn("Found existed entry log " + file + " when trying to create it as a new log.");
                            return true;
                        }
                        return false;
                    });

                if ((entrylogFilesWithPreallocatedLogId != null)
                    && (entrylogFilesWithPreallocatedLogId.length > 0)) {
                    entryLogAlreadyExistsWithThisId = true;
                    break;
                }
            }
            if (!entryLogAlreadyExistsWithThisId) {
                break;
            }
        }

        File newLogFile = new File(dirForNextEntryLog, logFileName);
        FileChannel channel = new RandomAccessFile(newLogFile, "rw").getChannel();
        EntryLogger.BufferedLogChannel logChannel = new EntryLogger.BufferedLogChannel(
                channel, conf.getWriteBufferBytes(), conf.getReadBufferBytes(),
                preallocatedLogId, newLogFile, conf.getFlushIntervalInBytes());
        logfileHeader.readerIndex(0);
        logChannel.write(logfileHeader);

        for (File f : ledgersDirs) {
            setLastLogId(f, preallocatedLogId);
        }
        if (suffix.equals(EntryLogger.LOG_FILE_SUFFIX)) {
            recentlyCreatedEntryLogsStatus.createdEntryLog(preallocatedLogId);
        }
        LOG.info("Created new entry log file {} for logId {}.", newLogFile, preallocatedLogId);
        return logChannel;
    }

    /**
     * Stop the allocator.
     */
    void stop() {
        // wait until the preallocation finished.
        allocatorExecutor.shutdown();
        LOG.info("Stopped entry logger preallocator.");
    }

    /**
     * get the preallocation for tests.
     */
    Future<EntryLogger.BufferedLogChannel> getPreallocationFuture(){
        return preallocation;
    }

    /**
     * writes the given id to the "lastId" file in the given directory.
     */
    private static void setLastLogId(File dir, long logId) throws IOException {
        FileOutputStream fos;
        fos = new FileOutputStream(new File(dir, "lastId"));
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos, UTF_8));
        try {
            bw.write(Long.toHexString(logId) + "\n");
            bw.flush();
        } catch (IOException e) {
            LOG.warn("Failed write lastId file");
        } finally {
            try {
                bw.close();
            } catch (IOException e) {
                LOG.error("Could not close lastId file in {}", dir.getPath());
            }
        }
    }
}
