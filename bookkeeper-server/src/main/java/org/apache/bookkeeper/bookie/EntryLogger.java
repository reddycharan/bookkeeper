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
import static org.apache.bookkeeper.util.BookKeeperConstants.MAX_LOG_SIZE_LIMIT;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongBinaryOperator;

import org.apache.bookkeeper.bookie.LedgerDirsManager.NoWritableLedgerDirException;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.commons.lang.mutable.MutableInt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.MapMaker;

/**
 * This class manages the writing of the bookkeeper entries. All the new
 * entries are written to a common log. The LedgerCache will have pointers
 * into files created by this class with offsets into the files to find
 * the actual ledger entry. The entry log files created by this class are
 * identified by a long.
 */
public class EntryLogger {
    private static final Logger LOG = LoggerFactory.getLogger(EntryLogger.class);
    private static final Long INVALID_LEDGERID = new Long(-1); 

    class BufferedLogChannel extends BufferedChannel {
        final private long logId;
        final private File file;

        private final EntryLogMetadata entryLogMetada;
        private Long ledgerId = INVALID_LEDGERID;

        public BufferedLogChannel(FileChannel fc, File file, int writeCapacity, int readCapacity, long logId,
                long unpersistedBytesBound) throws IOException {
            super(fc, writeCapacity, readCapacity, unpersistedBytesBound);
            this.logId = logId;
            this.file = file;
            this.entryLogMetada = new EntryLogMetadata(logId);
        }

        public long getLogId() {
            return logId;
        }

        public void registerWrittenEntry(long ledgerId, long entrySize) {
            entryLogMetada.addLedgerSize(ledgerId, entrySize);
        }

        public Map<Long, Long> getLedgersMap() {
            return entryLogMetada.getLedgersMap();
        }
        
        public File getFile() {
            return file;
        }
        
        public boolean isLedgerDirFull() {
            return ledgerDirsManager.isDirFull(file.getParentFile());
        }
        
        public Long getLedgerId() {
            return ledgerId;
        }

        public void setLedgerId(Long ledgerId) {
            this.ledgerId = ledgerId;
        }
    }

    private final LedgerDirsManager ledgerDirsManager;

    private final boolean entryLogPerLedgerEnabled;
    
    RecentEntryLogsStatus recentlyCreatedEntryLogsStatus;
    
    /**
     * The maximum size of a entry logger file.
     */
    final long logSizeLimit;
    final EntryLoggerAllocator entryLoggerAllocator;
    final EntryLogManager entryLogManager;
    private final boolean entryLogPreAllocationEnabled;
    private final CopyOnWriteArrayList<EntryLogListener> listeners
        = new CopyOnWriteArrayList<EntryLogListener>();

    private static final int HEADER_V0 = 0; // Old log file format (no ledgers map index)
    private static final int HEADER_V1 = 1; // Introduced ledger map index
    private static final int HEADER_CURRENT_VERSION = HEADER_V1;    

    private static class Header {
        final int version;
        final long ledgersMapOffset;
        final int ledgersCount;

        Header(int version, long ledgersMapOffset, int ledgersCount) {
            this.version = version;
            this.ledgersMapOffset = ledgersMapOffset;
            this.ledgersCount = ledgersCount;
        }
    }

    /**
     * The 1K block at the head of the entry logger file
     * that contains the fingerprint and meta-data.
     *
     * Header is composed of:
     * Fingerprint: 4 bytes "BKLO"
     * Log file HeaderVersion enum: 4 bytes
     * Ledger map offset: 8 bytes
     * Ledgers Count: 4 bytes
     */
    final static int LOGFILE_HEADER_SIZE = 1024;
    final ByteBuffer LOGFILE_HEADER = ByteBuffer.allocate(LOGFILE_HEADER_SIZE);

    final static int HEADER_VERSION_POSITION = 4;
    final static int LEDGERS_MAP_OFFSET_POSITION = HEADER_VERSION_POSITION + 4;

    /**
     * Ledgers map is composed of multiple parts that can be split into separated entries. Each of them is composed of:
     *
     * <pre>
     * length: (4 bytes) [0-3]
     * ledger id (-1): (8 bytes) [4 - 11]
     * entry id: (8 bytes) [12-19]
     * num ledgers stored in current metadata entry: (4 bytes) [20 - 23]
     * ledger entries: sequence of (ledgerid, size) (8 + 8 bytes each) [24..]
     * </pre>
     */
    final static int LEDGERS_MAP_HEADER_SIZE = 4 + 8 + 8 + 4;
    final static int LEDGERS_MAP_ENTRY_SIZE = 8 + 8;

    // Break the ledgers map into multiple batches, each of which can contain up to 10K ledgers
    final static int LEDGERS_MAP_MAX_BATCH_SIZE = 10000;

    final static long INVALID_LID = -1L;

    // EntryId used to mark an entry (belonging to INVALID_ID) as a component of the serialized ledgers map
    final static long LEDGERS_MAP_ENTRY_ID = -2L;

    final static int MIN_SANE_ENTRY_SIZE = 8 + 8;
    final static long MB = 1024 * 1024;

    private final long flushIntervalInBytes;
    private final boolean doRegularFlushes;

    final ServerConfiguration conf;
    /**
     * Scan entries in a entry log file.
     */
    static interface EntryLogScanner {
        /**
         * Tests whether or not the entries belongs to the specified ledger
         * should be processed.
         *
         * @param ledgerId
         *          Ledger ID.
         * @return true if and only the entries of the ledger should be scanned.
         */
        public boolean accept(long ledgerId);

        /**
         * Process an entry.
         *
         * @param ledgerId
         *          Ledger ID.
         * @param offset
         *          File offset of this entry.
         * @param entry
         *          Entry ByteBuffer
         * @throws IOException
         */
        public void process(long ledgerId, long offset, ByteBuffer entry) throws IOException;
    }

    /**
     * Entry Log Listener
     */
    static interface EntryLogListener {
        /**
         * Rotate a new entry log to write.
         */
        public void onRotateEntryLog();
    }

    /**
     * Create an EntryLogger that stores it's log files in the given
     * directories
     */
    public EntryLogger(ServerConfiguration conf,
            LedgerDirsManager ledgerDirsManager) throws IOException {
        this(conf, ledgerDirsManager, null);
    }

    public EntryLogger(ServerConfiguration conf,
            LedgerDirsManager ledgerDirsManager, EntryLogListener listener)
                    throws IOException {
        this.ledgerDirsManager = ledgerDirsManager;
        this.conf = conf;
        entryLogPerLedgerEnabled = conf.isEntryLogPerLedgerEnabled();
        
        if (listener != null) {
            addListener(listener);
        }
        // log size limit
        this.logSizeLimit = Math.min(conf.getEntryLogSizeLimit(), MAX_LOG_SIZE_LIMIT);
        this.entryLogPreAllocationEnabled = conf.isEntryLogFilePreAllocationEnabled();

        // Initialize the entry log header buffer. This cannot be a static object
        // since in our unit tests, we run multiple Bookies and thus EntryLoggers
        // within the same JVM. All of these Bookie instances access this header
        // so there can be race conditions when entry logs are rolled over and
        // this header buffer is cleared before writing it into the new logChannel.
        LOGFILE_HEADER.put("BKLO".getBytes(UTF_8));
        LOGFILE_HEADER.putInt(HEADER_CURRENT_VERSION);

        // Find the largest logId
        long logId = INVALID_LID;
        for (File dir : ledgerDirsManager.getAllLedgerDirs()) {
            if (!dir.exists()) {
                throw new FileNotFoundException(
                        "Entry log directory does not exist");
            }
            long lastLogId = getLastLogId(dir);
            if (lastLogId > logId) {
                logId = lastLogId;
            }
        }
        this.recentlyCreatedEntryLogsStatus = new RecentEntryLogsStatus(logId + 1);
        this.flushIntervalInBytes = conf.getFlushIntervalInBytes();
        this.doRegularFlushes = flushIntervalInBytes > 0;
        this.entryLoggerAllocator = new EntryLoggerAllocator(logId);
        if (entryLogPerLedgerEnabled) {
            this.entryLogManager = new EntryLogManagerForEntryLogPerLedger(conf);
        } else {
            this.entryLogManager = new EntryLogManagerForSingleEntryLog();
        }
    }
    
    void addListener(EntryLogListener listener) {
        if (null != listener) {
            listeners.add(listener);
        }
    }

    /**
     * If the log id of current writable channel is the same as entryLogId and the position
     * we want to read might end up reading from a position in the write buffer of the
     * buffered channel, route this read to the current logChannel. Else,
     * read from the BufferedReadChannel that is provided.
     * @param entryLogId
     * @param channel
     * @param buff remaining() on this bytebuffer tells us the last position that we
     *             expect to read.
     * @param pos The starting position from where we want to read.
     * @return
     */
    private int readFromLogChannel(long entryLogId, BufferedReadChannel channel, ByteBuffer buff, long pos)
            throws IOException {
        BufferedLogChannel bc = entryLogManager.getCurrentLogIfPresent(entryLogId);
        if (null != bc) {
            synchronized (bc) {
                if (pos + buff.remaining() >= bc.getFileChannelPosition()) {
                    return bc.read(buff, pos);
                }
            }
        }
        return channel.read(buff, pos);
    }

    /**
     * A thread-local variable that wraps a mapping of log ids to bufferedchannels
     * These channels should be used only for reading. logChannel is the one
     * that is used for writes.
     */
    private final ThreadLocal<Map<Long, BufferedReadChannel>> logid2Channel
            = new ThreadLocal<Map<Long, BufferedReadChannel>>() {
        @Override
        public Map<Long, BufferedReadChannel> initialValue() {
            // Since this is thread local there only one modifier
            // We dont really need the concurrency, but we need to use
            // the weak values. Therefore using the concurrency level of 1
            return new MapMaker().concurrencyLevel(1)
                .weakValues()
                .makeMap();
        }
    };

    /**
     * Each thread local buffered read channel can share the same file handle because reads are not relative
     * and don't cause a change in the channel's position. We use this map to store the file channels. Each
     * file channel is mapped to a log id which represents an open log file.
     */
    private final ConcurrentMap<Long, FileChannel> logid2FileChannel
            = new ConcurrentHashMap<Long, FileChannel>();

    /**
     * Put the logId, bc pair in the map responsible for the current thread.
     * @param logId
     * @param bc
     */
    public BufferedReadChannel putInReadChannels(long logId, BufferedReadChannel bc) {
        Map<Long, BufferedReadChannel> threadMap = logid2Channel.get();
        return threadMap.put(logId, bc);
    }

    /**
     * Remove all entries for this log file in each thread's cache.
     * @param logId
     */
    public void removeFromChannelsAndClose(long logId) {
        FileChannel fileChannel = logid2FileChannel.remove(logId);
        if (null != fileChannel) {
            try {
                fileChannel.close();
            } catch (IOException e) {
                LOG.warn("Exception while closing channel for log file:" + logId);
            }
        }
    }

    public BufferedReadChannel getFromChannels(long logId) {
        return logid2Channel.get().get(logId);
    }

    /**
     * Get the least unflushed log id. Garbage collector thread should not process
     * unflushed entry log file.
     *
     * @return least unflushed log id.
     */
    long getLeastUnflushedLogId() {
        return recentlyCreatedEntryLogsStatus.getLeastUnflushedLogId();
    }

    long getPreviousAllocatedEntryLogId() {
        return entryLoggerAllocator.getPreallocatedLogId();
    }

    void rollLogsIfEntryLogLimitReached() throws IOException {
        // for this add ledgerid to bufferedlogchannel, getcopyofcurrentlogs get
        // ledgerid, lock it only if there is new data
        // so that cache accesstime is not changed

        Set<BufferedLogChannel> copyOfCurrentLogs = entryLogManager.getCopyOfCurrentLogs();
        for (BufferedLogChannel currentLog : copyOfCurrentLogs) {
            if (currentLog.position() > logSizeLimit) {
                Long ledgerId = currentLog.getLedgerId();
                entryLogManager.acquireLock(ledgerId);
                try {
                    if (reachEntryLogLimit(ledgerId, 0L)) {
                        LOG.info("Rolling entry logger since it reached size limitation");
                        createNewLog(ledgerId);
                    }
                } finally {
                    entryLogManager.releaseLock(ledgerId);
                }
            }
        }
    }
    
    /**
     * Creates a new log file
     */
    void createNewLog(Long ledgerId) throws IOException {
        entryLogManager.acquireLock(ledgerId);
        try {
            BufferedLogChannel logChannel = entryLogManager.getCurrentLogForLedger(ledgerId);
            // first tried to create a new log channel. add current log channel to ToFlush list only when
            // there is a new log channel. it would prevent that a log channel is referenced by both
            // *logChannel* and *ToFlush* list.
            if (null != logChannel) {

                // flush the internal buffer back to filesystem but not sync disk
                logChannel.flush(false);

                // Append ledgers map at the end of entry log
                appendLedgersMap(ledgerId);

                BufferedLogChannel newLogChannel = entryLoggerAllocator.createNewLog();
                entryLogManager.setCurrentLogForLedger(ledgerId, newLogChannel);
                LOG.info("Flushing entry logger {} back to filesystem, pending for syncing entry loggers : {}.",
                        logChannel.getLogId(), entryLogManager.getCopyOfRotatedLogChannels());
                if (!entryLogPerLedgerEnabled) {
                    for (EntryLogListener listener : listeners) {
                        listener.onRotateEntryLog();
                    }
                }                
            } else {
                entryLogManager.setCurrentLogForLedger(ledgerId, entryLoggerAllocator.createNewLog());                
            }
        } finally {
            entryLogManager.releaseLock(ledgerId);
        }
    }

    /**
     * Append the ledger map at the end of the entry log.
     * Updates the entry log file header with the offset and size of the map.
     */
    private void appendLedgersMap(Long ledgerId) throws IOException {
        entryLogManager.acquireLock(ledgerId);
        try {
            BufferedLogChannel entryLogChannel = entryLogManager.getCurrentLogForLedger(ledgerId);

            long ledgerMapOffset = entryLogChannel.position();

            Map<Long, Long> ledgersMap = entryLogChannel.getLedgersMap();

            Iterator<Entry<Long, Long>> iterator = ledgersMap.entrySet().iterator();
            int numberOfLedgers = ledgersMap.size();
            int remainingLedgers = numberOfLedgers;

            // Write the ledgers map into several batches
            while (iterator.hasNext()) {
                // Start new batch
                int batchSize = Math.min(remainingLedgers, LEDGERS_MAP_MAX_BATCH_SIZE);
                int ledgerMapSize = LEDGERS_MAP_HEADER_SIZE + LEDGERS_MAP_ENTRY_SIZE * batchSize;
                ByteBuffer serializedMap = ByteBuffer.allocate(ledgerMapSize);

                serializedMap.putInt(ledgerMapSize - 4);
                serializedMap.putLong(INVALID_LID);
                serializedMap.putLong(LEDGERS_MAP_ENTRY_ID);
                serializedMap.putInt(batchSize);

                // Dump all ledgers for this batch
                for (int i = 0; i < batchSize; i++) {
                    Entry<Long, Long> entry = iterator.next();
                    long entryLedgerId = entry.getKey();
                    long size = entry.getValue();

                    serializedMap.putLong(entryLedgerId);
                    serializedMap.putLong(size);
                    --remainingLedgers;
                }

                // Close current batch
                serializedMap.flip();
                entryLogChannel.fileChannel.write(serializedMap);
            }

            // Update the headers with the map offset and count of ledgers
            ByteBuffer mapInfo = ByteBuffer.allocate(8 + 4);
            mapInfo.putLong(ledgerMapOffset);
            mapInfo.putInt(numberOfLedgers);
            mapInfo.flip();
            entryLogChannel.fileChannel.write(mapInfo, LEDGERS_MAP_OFFSET_POSITION);
        } finally {
            entryLogManager.releaseLock(ledgerId);
        }
    }

    /**
     * An allocator pre-allocates entry log files.
     */
    class EntryLoggerAllocator {

        long preallocatedLogId;
        Future<BufferedLogChannel> preallocation = null;
        ExecutorService allocatorExecutor;

        @SuppressWarnings("unchecked")
        EntryLoggerAllocator(long logId) {
            preallocatedLogId = logId;
            allocatorExecutor = Executors.newSingleThreadExecutor();
        }
        
        synchronized long getPreallocatedLogId(){
            return preallocatedLogId;
        }

        synchronized BufferedLogChannel createNewLog() throws IOException {
            BufferedLogChannel bc;
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
                    throw new IOException("Intrrupted when waiting a new entry log to be allocated.", ie);
                } catch (TimeoutException e) {
                    LOG.debug(
                            "Received TimeoutException while trying to get preallocation future result, which means that Future is waiting for acquiring lock on EntryLoggerAllocator.this");
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
                    preallocation = allocatorExecutor.submit(new Callable<BufferedLogChannel>() {
                        @Override
                        public BufferedLogChannel call() throws IOException {
                            return allocateNewLog();
                        }
                    });
                }
            }
            LOG.info("Created new entry logger {}.", bc.getLogId());
            return bc;
        }

        /**
         * Allocate a new log file.
         */
        synchronized BufferedLogChannel allocateNewLog() throws IOException {
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
                logFileName = Long.toHexString(preallocatedLogId) + ".log";
                boolean entryLogAlreadyExistsWithThisId = false;
                for (File dir : ledgersDirs) {
                    File newLogFile = new File(dir, logFileName);
                    if (newLogFile.exists()) {
                        LOG.warn("Found existed entry log " + newLogFile
                               + " when trying to create it as a new log.");
                        entryLogAlreadyExistsWithThisId = true;
                        break;
                    }
                }
                if(!entryLogAlreadyExistsWithThisId){
                    break;
                }
            }
            
            File newLogFile = new File(dirForNextEntryLog, logFileName);
            FileChannel channel = new RandomAccessFile(newLogFile, "rw").getChannel();
            BufferedLogChannel logChannel = new BufferedLogChannel(channel, newLogFile, conf.getWriteBufferBytes(),
                    conf.getReadBufferBytes(), preallocatedLogId, conf.getFlushIntervalInBytes());
            logChannel.write((ByteBuffer) LOGFILE_HEADER.clear());

            for (File f : ledgersDirs) {
                setLastLogId(f, preallocatedLogId);
            }
            recentlyCreatedEntryLogsStatus.createdEntryLog(preallocatedLogId);
            LOG.info("Preallocated entry logger {}.", preallocatedLogId);
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
    }

    /**
     * Remove entry log.
     *
     * @param entryLogId
     *          Entry Log File Id
     */
    protected boolean removeEntryLog(long entryLogId) {
        removeFromChannelsAndClose(entryLogId);
        File entryLogFile;
        try {
            entryLogFile = findFile(entryLogId);
        } catch (FileNotFoundException e) {
            LOG.error("Trying to delete an entryLog file that could not be found: "
                    + entryLogId + ".log");
            return false;
        }
        if (!entryLogFile.delete()) {
            LOG.warn("Could not delete entry log file {}", entryLogFile);
        }
        return true;
    }

    /**
     * writes the given id to the "lastId" file in the given directory.
     */
    private void setLastLogId(File dir, long logId) throws IOException {
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

    private long getLastLogId(File dir) {
        long id = readLastLogId(dir);
        // read success
        if (id > 0) {
            return id;
        }
        // read failed, scan the ledger directories to find biggest log id
        File[] logFiles = dir.listFiles(new FileFilter() {
            @Override
            public boolean accept(File file) {
                return file.getName().endsWith(".log");
            }
        });
        List<Long> logs = new ArrayList<Long>();
        if (logFiles != null) {
            for (File lf : logFiles) {
                String idString = lf.getName().split("\\.")[0];
                try {
                    long lid = Long.parseLong(idString, 16);
                    logs.add(lid);
                } catch (NumberFormatException nfe) {
                }
            }
        }
        // no log file found in this directory
        if (0 == logs.size()) {
            return INVALID_LID;
        }
        // order the collections
        Collections.sort(logs);
        return logs.get(logs.size() - 1);
    }

    /**
     * reads id from the "lastId" file in the given directory.
     */
    private long readLastLogId(File f) {
        FileInputStream fis;
        try {
            fis = new FileInputStream(new File(f, "lastId"));
        } catch (FileNotFoundException e) {
            return INVALID_LID;
        }
        BufferedReader br = new BufferedReader(new InputStreamReader(fis, UTF_8));
        try {
            String lastIdString = br.readLine();
            return Long.parseLong(lastIdString, 16);
        } catch (IOException e) {
            return INVALID_LID;
        } catch(NumberFormatException e) {
            return INVALID_LID;
        } finally {
            try {
                br.close();
            } catch (IOException e) {
            }
        }
    }

    interface EntryLogManager {
        /*
         * acquire lock for this ledger.
         */
        void acquireLock(Long ledgerId);

        /*
         * acquire lock for this ledger if it is not already available for this
         * ledger then it will create a new one and then acquire lock.
         */
        void acquireLockByCreatingIfRequired(Long ledgerId);

        /*
         * release lock for this ledger
         */
        void releaseLock(Long ledgerId);

        /*
         * sets the logChannel for the given ledgerId. The previous one will be
         * removed from replicaOfCurrentLogChannels. Previous logChannel will be
         * added to rotatedLogChannels.
         */
        void setCurrentLogForLedger(Long ledgerId, BufferedLogChannel logChannel);

        /*
         * gets the logChannel for the given ledgerId.
         */
        BufferedLogChannel getCurrentLogForLedger(Long ledgerId);

        /*
         * gets the copy of rotatedLogChannels
         */
        Set<BufferedLogChannel> getCopyOfRotatedLogChannels();

        /*
         * gets the copy of replicaOfCurrentLogChannels
         */
        Set<BufferedLogChannel> getCopyOfCurrentLogs();

        /*
         * gets the active logChannel with the given entryLogId. null if it is
         * not existing.
         */
        BufferedLogChannel getCurrentLogIfPresent(long entryLogId);

        /*
         * removes the logChannel from rotatedLogChannels collection
         */
        void removeFromRotatedLogChannels(BufferedLogChannel rotatedLogChannelToRemove);

        /*
         * Returns eligible writable ledger dir for the creation next entrylog
         */
        File getDirForNextEntryLog(List<File> writableLedgerDirs);
    }
    
    class EntryLogManagerForSingleEntryLog implements EntryLogManager {

        private BufferedLogChannel activeLogChannel;
        private Lock lockForActiveLogChannel;
        private final Set<BufferedLogChannel> rotatedLogChannels;

        EntryLogManagerForSingleEntryLog() {
            rotatedLogChannels = ConcurrentHashMap.newKeySet();
            lockForActiveLogChannel = new ReentrantLock();
        }

        /*
         * since entryLogPerLedger is not enabled, it is just one lock for all
         * ledgers.
         */
        @Override
        public void acquireLock(Long ledgerId) {
            lockForActiveLogChannel.lock();
        }

        @Override
        public void acquireLockByCreatingIfRequired(Long ledgerId) {
            acquireLock(ledgerId);
        }

        @Override
        public void releaseLock(Long ledgerId) {
            lockForActiveLogChannel.unlock();
        }

        @Override
        public void setCurrentLogForLedger(Long ledgerId, BufferedLogChannel logChannel) {
            acquireLock(ledgerId);
            try {
                BufferedLogChannel hasToRotateLogChannel = activeLogChannel;
                activeLogChannel = logChannel;
                if (hasToRotateLogChannel != null) {
                    rotatedLogChannels.add(hasToRotateLogChannel);
                }
            } finally {
                releaseLock(ledgerId);
            }
        }

        @Override
        public BufferedLogChannel getCurrentLogForLedger(Long ledgerId) {
            return activeLogChannel;
        }

        @Override
        public Set<BufferedLogChannel> getCopyOfRotatedLogChannels() {
            return new HashSet<BufferedLogChannel>(rotatedLogChannels);
        }

        @Override
        public Set<BufferedLogChannel> getCopyOfCurrentLogs() {
            HashSet<BufferedLogChannel> copyOfCurrentLogs = new HashSet<BufferedLogChannel>();
            copyOfCurrentLogs.add(activeLogChannel);
            return copyOfCurrentLogs;
        }

        @Override
        public BufferedLogChannel getCurrentLogIfPresent(long entryLogId) {
            BufferedLogChannel activeLogChannelTemp = activeLogChannel;
            if ((activeLogChannelTemp != null) && (activeLogChannelTemp.getLogId() == entryLogId)) {
                return activeLogChannelTemp;
            }
            return null;
        }

        @Override
        public void removeFromRotatedLogChannels(BufferedLogChannel rotatedLogChannelToRemove) {
            rotatedLogChannels.remove(rotatedLogChannelToRemove);
        }

        @Override
        public File getDirForNextEntryLog(List<File> writableLedgerDirs) {
            Collections.shuffle(writableLedgerDirs);
            return writableLedgerDirs.get(0);
        }
    }
    
    class EntryLogManagerForEntryLogPerLedger implements EntryLogManager {

        class EntryLogAndLockTuple {
            private final Lock ledgerLock;
            private BufferedLogChannel entryLog;

            public EntryLogAndLockTuple() {
                ledgerLock = new ReentrantLock();
            }

            public Lock getLedgerLock() {
                return ledgerLock;
            }

            public BufferedLogChannel getEntryLog() {
                return entryLog;
            }

            public void setEntryLog(BufferedLogChannel entryLog) {
                this.entryLog = entryLog;
            }
        }
        
        private Cache<Long, EntryLogAndLockTuple> ledgerIdEntryLogMap;
        private final Set<BufferedLogChannel> rotatedLogChannels;
        /*
         * every time active logChannel is accessed from ledgerIdEntryLogMap
         * cache, the accesstime of that entry is updated. But for certain
         * operations we dont want to impact accessTime of the entries (like
         * periodic flush of current active logChannels), and those operations
         * can use this copy of references.
         */
        private final ConcurrentHashMap<Long, BufferedLogChannel> replicaOfCurrentLogChannels;
        private final Callable<EntryLogAndLockTuple> entryLogAndLockTupleValueLoader;

        EntryLogManagerForEntryLogPerLedger(ServerConfiguration conf) throws IOException {
            rotatedLogChannels = ConcurrentHashMap.newKeySet();

            replicaOfCurrentLogChannels = new ConcurrentHashMap<Long, BufferedLogChannel>();
            int entrylogMapAccessExpiryTimeInSeconds = conf.getEntrylogMapAccessExpiryTimeInSeconds();
            entryLogAndLockTupleValueLoader = new Callable<EntryLogAndLockTuple>() {
                @Override
                public EntryLogAndLockTuple call() throws Exception {
                    return new EntryLogAndLockTuple();
                }
            };
            /*
             * Currently we are relying on access time based eviction policy for
             * removal of EntryLogAndLockTuple, so if the EntryLogAndLockTuple of
             * the ledger is not accessed in
             * entrylogMapAccessExpiryTimeInSeconds period, it will be removed
             * from the cache.
             * 
             * We are going to introduce explicit advisory writeClose call, with
             * that explicit call EntryLogAndLockTuple of the ledger will be
             * removed from the cache. But still timebased eviciton policy is
             * needed because it is not guaranteed that Bookie/EntryLogger would
             * receive successfully write close call in all the cases.
             */
            ledgerIdEntryLogMap = CacheBuilder.newBuilder()
                    .expireAfterAccess(entrylogMapAccessExpiryTimeInSeconds, TimeUnit.SECONDS)
                    .removalListener(new RemovalListener<Long, EntryLogAndLockTuple>() {
                        @Override
                        public void onRemoval(
                                RemovalNotification<Long, EntryLogAndLockTuple> expiredLedgerEntryLogMapEntry) {
                            removalOnExpiry(expiredLedgerEntryLogMapEntry);
                        }
                    }).build();
        }

        /*
         * This method is called when access time of that ledger has elapsed
         * entrylogMapAccessExpiryTimeInSeconds period and the entry for that
         * ledger is removed from cache. Since the entrylog of this ledger is
         * not active anymore it has to be removed from
         * replicaOfCurrentLogChannels and added to rotatedLogChannels.
         * 
         * Because of performance/optimizations concerns the cleanup maintenance
         * operations wont happen automatically, for more info on eviction
         * cleanup maintenance tasks -
         * https://google.github.io/guava/releases/19.0/api/docs/com/google/
         * common/cache/CacheBuilder.html
         * 
         */
        private void removalOnExpiry(RemovalNotification<Long, EntryLogAndLockTuple> expiredLedgerEntryLogMapEntry) {
            Long ledgerId = expiredLedgerEntryLogMapEntry.getKey();
            LOG.debug(
                    "LedgerId {} is not accessed for entrylogMapAccessExpiryTimeInSeconds period so it is being evicted from the cache map",
                    ledgerId);
            EntryLogAndLockTuple entryLogAndLockTuple = expiredLedgerEntryLogMapEntry.getValue();
            Lock lock = entryLogAndLockTuple.ledgerLock;
            BufferedLogChannel logChannel = entryLogAndLockTuple.entryLog;
            lock.lock();
            try {
                replicaOfCurrentLogChannels.remove(logChannel.logId);
                rotatedLogChannels.add(logChannel);
            } finally {
                lock.unlock();
            }
        }

        @Override
        public void acquireLock(Long ledgerId) {
            ledgerIdEntryLogMap.getIfPresent(ledgerId).getLedgerLock().lock();
        }

        /*
         * acquire lock for this ledger. In this method if EntryLogAndLockTuple
         * is not already available for this ledger in the cache, then it will
         * create a new EntryLogAndLockTuple, add it to cache and acquire lock.
         * 
         */
        @Override
        public void acquireLockByCreatingIfRequired(Long ledgerId) {
            try {
                ledgerIdEntryLogMap.get(ledgerId, entryLogAndLockTupleValueLoader).getLedgerLock().lock();
            } catch (ExecutionException e) {
                throw new RuntimeException(
                        "Got ExecutionException while trying to create EntryLogAndLockTuple for Ledger: " + ledgerId,
                        e);
            }
        }

        @Override
        public void releaseLock(Long ledgerId) {
            ledgerIdEntryLogMap.getIfPresent(ledgerId).getLedgerLock().unlock();
        }

        /*
         * sets the logChannel for the given ledgerId. It will add the new
         * logchannel to replicaOfCurrentLogChannels, and the previous one will
         * be removed from replicaOfCurrentLogChannels. Previous logChannel will
         * be added to rotatedLogChannels in both the cases.
         */
        @Override
        public void setCurrentLogForLedger(Long ledgerId, BufferedLogChannel logChannel) {
            acquireLock(ledgerId);
            try {
                BufferedLogChannel hasToRotateLogChannel = getCurrentLogForLedger(ledgerId);
                logChannel.setLedgerId(ledgerId);
                ledgerIdEntryLogMap.getIfPresent(ledgerId).setEntryLog(logChannel);
                replicaOfCurrentLogChannels.put(logChannel.logId, logChannel);
                if (hasToRotateLogChannel != null) {
                    replicaOfCurrentLogChannels.remove(hasToRotateLogChannel.logId);
                    rotatedLogChannels.add(hasToRotateLogChannel);
                }
            } finally {
                releaseLock(ledgerId);
            }
        }

        @Override
        public BufferedLogChannel getCurrentLogForLedger(Long ledgerId) {
            EntryLogAndLockTuple entryLogAndLockTuple = ledgerIdEntryLogMap.getIfPresent(ledgerId);
            if (entryLogAndLockTuple == null) {
                return null;
            } else {
                return entryLogAndLockTuple.getEntryLog();
            }
        }

        @Override
        public Set<BufferedLogChannel> getCopyOfRotatedLogChannels() {
            return new HashSet<BufferedLogChannel>(rotatedLogChannels);
        }

        @Override
        public Set<BufferedLogChannel> getCopyOfCurrentLogs() {
            return new HashSet<BufferedLogChannel>(replicaOfCurrentLogChannels.values());
        }

        @Override
        public BufferedLogChannel getCurrentLogIfPresent(long entryLogId) {
            return replicaOfCurrentLogChannels.get(entryLogId);
        }

        @Override
        public void removeFromRotatedLogChannels(BufferedLogChannel rotatedLogChannelToRemove) {
            rotatedLogChannels.remove(rotatedLogChannelToRemove);
        }

        /*
         * this is for testing purpose only. guava's cache doesnt cleanup
         * completely (including calling expiry removal listener) automatically
         * when access timeout elapses.
         * 
         * https://google.github.io/guava/releases/19.0/api/docs/com/google/
         * common/cache/CacheBuilder.html
         * 
         * If expireAfterWrite or expireAfterAccess is requested entries may be
         * evicted on each cache modification, on occasional cache accesses, or
         * on calls to Cache.cleanUp(). Expired entries may be counted by
         * Cache.size(), but will never be visible to read or write operations.
         * 
         * Certain cache configurations will result in the accrual of periodic
         * maintenance tasks which will be performed during write operations, or
         * during occasional read operations in the absence of writes. The
         * Cache.cleanUp() method of the returned cache will also perform
         * maintenance, but calling it should not be necessary with a high
         * throughput cache. Only caches built with removalListener,
         * expireAfterWrite, expireAfterAccess, weakKeys, weakValues, or
         * softValues perform periodic maintenance.
         */
        void doEntryLogMapCleanup() {
            ledgerIdEntryLogMap.cleanUp();
        }

        /*
         * Returns writable ledger dir with least number of current active
         * entrylogs.
         */
        @Override
        public File getDirForNextEntryLog(List<File> writableLedgerDirs) {
            Map<File, MutableInt> writableLedgerDirFrequency = new HashMap<File, MutableInt>();
            writableLedgerDirs.stream()
                    .forEach((ledgerDir) -> writableLedgerDirFrequency.put(ledgerDir, new MutableInt()));
            for (BufferedLogChannel logChannel : replicaOfCurrentLogChannels.values()) {
                File parentDirOfCurrentLogChannel = logChannel.getFile().getParentFile();
                if (writableLedgerDirFrequency.containsKey(parentDirOfCurrentLogChannel)) {
                    writableLedgerDirFrequency.get(parentDirOfCurrentLogChannel).increment();
                }
            }
            @SuppressWarnings("unchecked")
            Optional<Entry<File, MutableInt>> ledgerDirWithLeastNumofCurrentLogs = writableLedgerDirFrequency.entrySet()
                    .stream().min(Map.Entry.comparingByValue());
            return ledgerDirWithLeastNumofCurrentLogs.get().getKey();
        }
    }
    
    /**
     * Flushes all rotated log channels.
     */
    void checkpoint() throws IOException {
        flushRotatedLogs();
    }

    void flushRotatedLogs() throws IOException {
        Set<BufferedLogChannel> channels = entryLogManager.getCopyOfRotatedLogChannels();
        if (null == channels) {
            return;
        }
        for (BufferedLogChannel channel : channels) {
            channel.flush(true);
            // since this channel is only used for writing, after flushing the channel,
            // we had to close the underlying file channel. Otherwise, we might end up
            // leaking fds which cause the disk spaces could not be reclaimed.
            closeFileChannel(channel);
            recentlyCreatedEntryLogsStatus.flushRotatedEntryLog(channel.getLogId());
            entryLogManager.removeFromRotatedLogChannels(channel);
            LOG.info("Synced entry logger {} to disk.", channel.getLogId());
        }
    }

    void flush() throws IOException {
        flushCurrentLogs();
        flushRotatedLogs();
    }

    void flushCurrentLogs() throws IOException {
        Set<BufferedLogChannel> copyOfCurrentLogs = entryLogManager.getCopyOfCurrentLogs();
        for (BufferedLogChannel logChannel : copyOfCurrentLogs) {
            /**
             * flushCurrentLogs method is called during checkpoint, so metadata
             * of the file also should be force written.
             */
            flushCurrentLog(logChannel, true);
        }
    }

    void flushCurrentLog(BufferedLogChannel logChannel, boolean forceMetadata) throws IOException {
        if (logChannel != null) {
            logChannel.flush(true, forceMetadata);
            LOG.debug("Flush and sync current entry logger {}", logChannel.getLogId());
        }
    }

    long addEntry(Long ledger, ByteBuffer entry) throws IOException {
        return addEntry(ledger, entry, true);
    }

    long addEntry(Long ledger, ByteBuffer entry, boolean rollLog) throws IOException {
        entryLogManager.acquireLockByCreatingIfRequired(ledger);
        try {
            int entrySize = entry.remaining() + 4;
            boolean reachEntryLogLimit = rollLog ? reachEntryLogLimit(ledger, entrySize)
                    : readEntryLogHardLimit(ledger, entrySize);
            BufferedLogChannel logChannel = entryLogManager.getCurrentLogForLedger(ledger);
            // Create new log if logSizeLimit reached or current disk is full
            boolean diskFull = (logChannel == null) ? false : logChannel.isLedgerDirFull();
            boolean allDisksFull = !ledgerDirsManager.hasWritableLedgerDirs();
            
            /**
             * if disk of the logChannel is full or if the entrylog limit is
             * reached of if the logchannel is not initialized, then
             * createNewLog. If allDisks are full then proceed with the current
             * logChannel, since Bookie must have turned to readonly mode and
             * the addEntry traffic would be from GC and it is ok to proceed in
             * this case.
             */
            if ((diskFull && (!allDisksFull)) || reachEntryLogLimit || (logChannel == null)) {
                flushCurrentLog(logChannel, false);
                createNewLog(ledger);
            }

            logChannel = entryLogManager.getCurrentLogForLedger(ledger);
            ByteBuffer buff = ByteBuffer.allocate(4);
            buff.putInt(entry.remaining());
            buff.flip();

            logChannel.write(buff);
            long pos = logChannel.position();
            logChannel.write(entry);
            logChannel.registerWrittenEntry(ledger, entrySize);

            return (logChannel.getLogId() << 32L) | pos;
        } finally {
            entryLogManager.releaseLock(ledger);
        }
    }

    static long logIdForOffset(long offset) {
        return offset >> 32L;
    }

    boolean reachEntryLogLimit(Long ledger, long size) {
        entryLogManager.acquireLock(ledger);
        try {
            BufferedLogChannel logChannel = entryLogManager.getCurrentLogForLedger(ledger);
            if (logChannel == null) {
                return false;
            }
            return logChannel.position() + size > logSizeLimit;
        } finally {
            entryLogManager.releaseLock(ledger);
        }
    }

    boolean readEntryLogHardLimit(Long ledger, long size) {
        entryLogManager.acquireLock(ledger);
        try {
            BufferedLogChannel logChannel = entryLogManager.getCurrentLogForLedger(ledger);
            if (logChannel == null) {
                return false;
            }
            return logChannel.position() + size > Integer.MAX_VALUE;
        } finally {
            entryLogManager.releaseLock(ledger);
        }
    }

    byte[] readEntry(long ledgerId, long entryId, long location) throws IOException, Bookie.NoEntryException {
        long entryLogId = logIdForOffset(location);
        long pos = location & 0xffffffffL;
        ByteBuffer sizeBuff = ByteBuffer.allocate(4);
        pos -= 4; // we want to get the ledgerId and length to check
        BufferedReadChannel fc;
        try {
            fc = getChannelForLogId(entryLogId);
        } catch (FileNotFoundException e) {
            FileNotFoundException newe = new FileNotFoundException(e.getMessage() + " for " + ledgerId + " with location " + location);
            newe.setStackTrace(e.getStackTrace());
            throw newe;
        }
        if (readFromLogChannel(entryLogId, fc, sizeBuff, pos) != sizeBuff.capacity()) {
            throw new Bookie.NoEntryException("Short read from entrylog " + entryLogId,
                                              ledgerId, entryId);
        }
        pos += 4;
        sizeBuff.flip();
        int entrySize = sizeBuff.getInt();
        // entrySize does not include the ledgerId
        if (entrySize > MB) {
            LOG.error("Sanity check failed for entry size of " + entrySize + " at location " + pos + " in " + entryLogId);

        }
        if (entrySize < MIN_SANE_ENTRY_SIZE) {
            LOG.error("Read invalid entry length {}", entrySize);
            throw new IOException("Invalid entry length " + entrySize);
        }
        byte data[] = new byte[entrySize];
        ByteBuffer buff = ByteBuffer.wrap(data);
        int rc = readFromLogChannel(entryLogId, fc, buff, pos);
        if ( rc != data.length) {
            // Note that throwing NoEntryException here instead of IOException is not
            // without risk. If all bookies in a quorum throw this same exception
            // the client will assume that it has reached the end of the ledger.
            // However, this may not be the case, as a very specific error condition
            // could have occurred, where the length of the entry was corrupted on all
            // replicas. However, the chance of this happening is very very low, so
            // returning NoEntryException is mostly safe.
            throw new Bookie.NoEntryException("Short read for " + ledgerId + "@"
                                              + entryId + " in " + entryLogId + "@"
                                              + pos + "("+rc+"!="+data.length+")", ledgerId, entryId);
        }
        buff.flip();
        long thisLedgerId = buff.getLong();
        if (thisLedgerId != ledgerId) {
            throw new IOException("problem found in " + entryLogId + "@" + entryId + " at position + " + pos + " entry belongs to " + thisLedgerId + " not " + ledgerId);
        }
        long thisEntryId = buff.getLong();
        if (thisEntryId != entryId) {
            throw new IOException("problem found in " + entryLogId + "@" + entryId + " at position + " + pos + " entry is " + thisEntryId + " not " + entryId);
        }

        return data;
    }

    /**
     * Read the header of an entry log
     */
    private Header getHeaderForLogId(long entryLogId) throws IOException {
        BufferedReadChannel bc = getChannelForLogId(entryLogId);

        // Allocate buffer to read (version, ledgersMapOffset, ledgerCount)
        ByteBuffer headers = ByteBuffer.allocate(LOGFILE_HEADER_SIZE);
        bc.read(headers, 0);
        headers.flip();

        // Skip marker string "BKLO"
        headers.getInt();

        int headerVersion = headers.getInt();
        if (headerVersion < HEADER_V0 || headerVersion > HEADER_CURRENT_VERSION) {
            LOG.info("Unknown entry log header version for log {}: {}", entryLogId, headerVersion);
        }

        long ledgersMapOffset = headers.getLong();
        int ledgersCount = headers.getInt();
        return new Header(headerVersion, ledgersMapOffset, ledgersCount);
    }

    private BufferedReadChannel getChannelForLogId(long entryLogId) throws IOException {
        BufferedReadChannel fc = getFromChannels(entryLogId);
        if (fc != null) {
            return fc;
        }
        File file = findFile(entryLogId);
        // get channel is used to open an existing entry log file
        // it would be better to open using read mode
        FileChannel newFc = new RandomAccessFile(file, "r").getChannel();
        FileChannel oldFc = logid2FileChannel.putIfAbsent(entryLogId, newFc);
        if (null != oldFc) {
            newFc.close();
            newFc = oldFc;
        }
        // We set the position of the write buffer of this buffered channel to Long.MAX_VALUE
        // so that there are no overlaps with the write buffer while reading
        fc = new BufferedReadChannel(newFc, conf.getReadBufferBytes());
        putInReadChannels(entryLogId, fc);
        return fc;
    }

    /**
     * Whether the log file exists or not.
     */
    boolean logExists(long logId) {
        for (File d : ledgerDirsManager.getAllLedgerDirs()) {
            File f = new File(d, Long.toHexString(logId) + ".log");
            if (f.exists()) {
                return true;
            }
        }
        return false;
    }

    private File findFile(long logId) throws FileNotFoundException {
        for (File d : ledgerDirsManager.getAllLedgerDirs()) {
            File f = new File(d, Long.toHexString(logId)+".log");
            if (f.exists()) {
                return f;
            }
        }
        throw new FileNotFoundException("No file for log " + Long.toHexString(logId));
    }

    /**
     * Scan entry log
     *
     * @param entryLogId
     *          Entry Log Id
     * @param scanner
     *          Entry Log Scanner
     * @throws IOException
     */
    protected void scanEntryLog(long entryLogId, EntryLogScanner scanner) throws IOException {
        ByteBuffer sizeBuff = ByteBuffer.allocate(4);
        ByteBuffer lidBuff = ByteBuffer.allocate(8);
        BufferedReadChannel bc;
        // Get the BufferedChannel for the current entry log file
        try {
            bc = getChannelForLogId(entryLogId);
        } catch (IOException e) {
            LOG.warn("Failed to get channel to scan entry log: " + entryLogId + ".log");
            throw e;
        }
        // Start the read position in the current entry log file to be after
        // the header where all of the ledger entries are.
        long pos = LOGFILE_HEADER_SIZE;

        // Read through the entry log file and extract the ledger ID's.
        while (true) {
            // Check if we've finished reading the entry log file.
            if (pos >= bc.size()) {
                break;
            }
            if (readFromLogChannel(entryLogId, bc, sizeBuff, pos) != sizeBuff.capacity()) {
                LOG.warn("Short read for entry size from entrylog {}", entryLogId);
                return;
            }
            long offset = pos;
            pos += 4;
            sizeBuff.flip();
            int entrySize = sizeBuff.getInt();

            sizeBuff.clear();
            // try to read ledger id first
            if (readFromLogChannel(entryLogId, bc, lidBuff, pos) != lidBuff.capacity()) {
                LOG.warn("Short read for ledger id from entrylog {}", entryLogId);
                return;
            }
            lidBuff.flip();
            long lid = lidBuff.getLong();
            lidBuff.clear();
            if (lid == INVALID_LID || !scanner.accept(lid)) {
                // skip this entry
                pos += entrySize;
                continue;
            }
            // read the entry
            byte data[] = new byte[entrySize];
            ByteBuffer buff = ByteBuffer.wrap(data);
            int rc = readFromLogChannel(entryLogId, bc, buff, pos);
            if (rc != data.length) {
                LOG.warn("Short read for ledger entry from entryLog {}@{} ({} != {})", new Object[] { entryLogId, pos,
                        rc, data.length });
                return;
            }
            buff.flip();
            // process the entry
            scanner.process(lid, offset, buff);
            // Advance position to the next entry
            pos += entrySize;
        }
    }

    public EntryLogMetadata getEntryLogMetadata(long entryLogId) throws IOException {
        // First try to extract the EntryLogMetada from the index, if there's no index then fallback to scanning the
        // entry log
        try {
            return extractEntryLogMetadataFromIndex(entryLogId);
        } catch (Exception e) {
            LOG.info("Failed to get ledgers map index from: {}.log : {}", entryLogId, e.getMessage());

            // Fall-back to scanning
            return extractEntryLogMetadataByScanning(entryLogId);
        }
    }

    EntryLogMetadata extractEntryLogMetadataFromIndex(long entryLogId) throws IOException {
        Header header = getHeaderForLogId(entryLogId);

        if (header.version < HEADER_V1) {
            throw new IOException("Old log file header without ledgers map on entryLogId " + entryLogId);
        }

        if (header.ledgersMapOffset == 0L) {
            // The index was not stored in the log file (possibly because the bookie crashed before flushing it)
            throw new IOException("No ledgers map index found on entryLogId" + entryLogId);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Recovering ledgers maps for log {} at offset: {}", entryLogId, header.ledgersMapOffset);
        }

        BufferedReadChannel bc = getChannelForLogId(entryLogId);

        // There can be multiple entries containing the various components of the serialized ledgers map
        long offset = header.ledgersMapOffset;
        EntryLogMetadata meta = new EntryLogMetadata(entryLogId);

        while (offset < bc.size()) {
            // Read ledgers map size
            ByteBuffer sizeBuf = ByteBuffer.allocate(4);
            bc.read(sizeBuf, offset);
            sizeBuf.flip();

            int ledgersMapSize = sizeBuf.getInt();

            // Read the index into a buffer
            ByteBuffer ledgersMapBuffer = ByteBuffer.allocate(ledgersMapSize);
            bc.read(ledgersMapBuffer, offset + 4);
            ledgersMapBuffer.flip();

            // Discard ledgerId and entryId
            long lid = ledgersMapBuffer.getLong();
            if (lid != INVALID_LID) {
                throw new IOException("Cannot deserialize ledgers map from ledger " + lid + " -- entryLogId: " + entryLogId);
            }

            long entryId = ledgersMapBuffer.getLong();
            if (entryId != LEDGERS_MAP_ENTRY_ID) {
                throw new IOException("Cannot deserialize ledgers map from ledger " + lid + ":" + entryId + " -- entryLogId: " + entryLogId);
            }

            // Read the number of ledgers in the current entry batch
            int ledgersCount = ledgersMapBuffer.getInt();

            // Extract all (ledger,size) tuples from buffer
            for (int i = 0; i < ledgersCount; i++) {
                long ledgerId = ledgersMapBuffer.getLong();
                long size = ledgersMapBuffer.getLong();

                if (LOG.isDebugEnabled()) {
                    LOG.debug("Recovering ledgers maps for log {} -- Found ledger: {} with size: {}",
                            new Object[] { entryLogId, ledgerId, size });
                }
                meta.addLedgerSize(ledgerId, size);
            }

            if (ledgersMapBuffer.hasRemaining()) {
                throw new IOException("Invalid entry size when reading ledgers map on entryLogId: " + entryLogId);
            }

            // Move to next entry, if any
            offset += ledgersMapSize + 4;
        }

        if (meta.getLedgersMap().size() != header.ledgersCount) {
            throw new IOException("Not all ledgers were found in ledgers map index. expected: " + header.ledgersCount
                    + " -- found: " + meta.getLedgersMap().size() + " -- entryLogId: " + entryLogId);
        }

        return meta;
    }

    private EntryLogMetadata extractEntryLogMetadataByScanning(long entryLogId) throws IOException {
        final EntryLogMetadata meta = new EntryLogMetadata(entryLogId);

        // Read through the entry log file and extract the entry log meta
        scanEntryLog(entryLogId, new EntryLogScanner() {
            @Override
            public void process(long ledgerId, long offset, ByteBuffer entry) throws IOException {
                // add new entry size of a ledger to entry log meta
                meta.addLedgerSize(ledgerId, entry.limit() + 4);
            }

            @Override
            public boolean accept(long ledgerId) {
                return true;
            }
        });

        LOG.debug("Retrieved entry log meta data entryLogId: {}, meta: {}", entryLogId, meta);
        return meta;
    }

    /**
     * Shutdown method to gracefully stop entry logger.
     */
    public void shutdown() {
        // since logChannel is buffered channel, do flush when shutting down
        LOG.info("Stopping EntryLogger");
        Set<BufferedLogChannel> copyOfCurrentLogs = entryLogManager.getCopyOfCurrentLogs();
        try {
            flush();
            for (FileChannel fc : logid2FileChannel.values()) {
                fc.close();
            }
            // clear the mapping, so we don't need to go through the channels again in finally block in normal case.
            logid2FileChannel.clear();            
            for (BufferedLogChannel currentLog : copyOfCurrentLogs) {
                // close current writing log file
                closeFileChannel(currentLog);
            }
        } catch (IOException ie) {
            // we have no idea how to avoid io exception during shutting down, so just ignore it
            LOG.error("Error flush entry log during shutting down, which may cause entry log corrupted.", ie);
        } finally {
            for (FileChannel fc : logid2FileChannel.values()) {
                IOUtils.close(LOG, fc);
            }
            for (BufferedLogChannel currentLog : copyOfCurrentLogs) {
                forceCloseFileChannel(currentLog);
            }
        }
        // shutdown the pre-allocation thread
        entryLoggerAllocator.stop();
    }

    private static void closeFileChannel(BufferedChannelBase channel) throws IOException {
        if (null == channel) {
            return;
        }
        FileChannel fileChannel = channel.getFileChannel();
        if (null != fileChannel) {
            fileChannel.close();
        }
    }

    private static void forceCloseFileChannel(BufferedChannelBase channel) {
        if (null == channel) {
            return;
        }
        FileChannel fileChannel = channel.getFileChannel();
        if (null != fileChannel) {
            IOUtils.close(LOG, fileChannel);
        }
    }

    /**
     * Datastructure which maintains the status of logchannels. When a
     * logChannel is created entry of <entryLogId, false> will be made to this
     * sortedmap and when logChannel is rotated and flushed then the entry is
     * updated to <entryLogId, true> and all the lowest entries with
     * <entryLogId, true> status will be removed from the sortedmap. So that way 
     * we could get least unflushed LogId.
     *
     */
    class RecentEntryLogsStatus {
        private SortedMap<Long, Boolean> entryLogsStatusMap;
        private long leastUnflushedLogId;

        RecentEntryLogsStatus(long leastUnflushedLogId) {
            entryLogsStatusMap = new TreeMap<Long, Boolean>();
            this.leastUnflushedLogId = leastUnflushedLogId;
        }

        synchronized void createdEntryLog(Long entryLogId) {
            entryLogsStatusMap.put(entryLogId, false);
        }

        synchronized void flushRotatedEntryLog(Long entryLogId) {
            entryLogsStatusMap.replace(entryLogId, true);
            while ((!entryLogsStatusMap.isEmpty()) && (entryLogsStatusMap.get(entryLogsStatusMap.firstKey()))) {
                long leastFlushedLogId = entryLogsStatusMap.firstKey();
                entryLogsStatusMap.remove(leastFlushedLogId);
                leastUnflushedLogId = leastFlushedLogId + 1;
            }
        }

        synchronized long getLeastUnflushedLogId() {
            return leastUnflushedLogId;
        }
    }
}
