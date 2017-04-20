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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
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
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;
import java.util.concurrent.locks.StampedLock;

import org.apache.bookkeeper.bookie.LedgerDirsManager.NoWritableLedgerDirException;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.util.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
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

    static class BufferedLogChannel extends BufferedChannel {
        private final long logId;
        private final File file;
        private final EntryLogMetadata entryLogMetadata;

        public BufferedLogChannel(FileChannel fc, File file, int writeCapacity,
                                  int readCapacity, long logId) throws IOException {
            super(fc, writeCapacity, readCapacity);
            this.logId = logId;
            this.file = file;
            this.entryLogMetadata = new EntryLogMetadata(logId);
        }

        public long getLogId() {
            return logId;
        }

        public void registerWrittenEntry(long ledgerId, long entrySize) {
            entryLogMetadata.addLedgerSize(ledgerId, entrySize);
        }

        public Map<Long, Long> getLedgersMap() {
            return entryLogMetadata.getLedgersMap();
        }
        
        public File getFile() {
            return file;
        }
    }

    //volatile File currentDir;
    private final LedgerDirsManager ledgerDirsManager;

    private final int numberOfActiveEntryLogsPerLedgerDir;
    private final int totalNumberOfActiveEntryLogs;
    
    //private volatile long leastUnflushedLogId;
    RecentEntryLogsStatus recentlyCreatedEntryLogsStatus;
    
    /**
     * The maximum size of a entry logger file.
     */
    final long logSizeLimit;
    final EntryLoggerAllocator entryLoggerAllocator;
    final EntryLogManager entryLogManager;
    private final boolean entryLogPreAllocationEnabled;
    private final CopyOnWriteArrayList<EntryLogListener> listeners = new CopyOnWriteArrayList<EntryLogListener>();
    // all the operations on activeEntryLogs should be guarded by these locks. 
    private final ReentrantLock[] locksForEntryLogs;

    private static final int HEADER_V0 = 0; // Old log file format (no ledgers map index)
    private static final int HEADER_V1 = 1; // Introduced ledger map index
    private static final int HEADER_CURRENT_VERSION = HEADER_V1;
    private static final int ENTRYLOGMAP_ACCESS_EXPIRYTIME_INMINUTES = 30;

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
     * <pre>
     * Header is composed of:
     * Fingerprint: 4 bytes "BKLO"
     * Log file HeaderVersion enum: 4 bytes
     * Ledger map offset: 8 bytes
     * Ledgers Count: 4 bytes
     * </pre>
     */
    static final int LOGFILE_HEADER_SIZE = 1024;
    final ByteBuffer logfileHeader = ByteBuffer.allocate(LOGFILE_HEADER_SIZE);

    static final int HEADER_VERSION_POSITION = 4;
    static final int LEDGERS_MAP_OFFSET_POSITION = HEADER_VERSION_POSITION + 4;

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
    static final int LEDGERS_MAP_HEADER_SIZE = 4 + 8 + 8 + 4;
    static final int LEDGERS_MAP_ENTRY_SIZE = 8 + 8;

    // Break the ledgers map into multiple batches, each of which can contain up to 10K ledgers
    static final int LEDGERS_MAP_MAX_BATCH_SIZE = 10000;

    static final long INVALID_LID = -1L;

    // EntryId used to mark an entry (belonging to INVALID_ID) as a component of the serialized ledgers map
    static final long LEDGERS_MAP_ENTRY_ID = -2L;

    static final int MIN_SANE_ENTRY_SIZE = 8 + 8;
    static final long MB = 1024 * 1024;

    private final long flushIntervalInBytes;
    private final boolean doRegularFlushes;
    final AtomicLongArray bytesWrittenSinceLastFlush;
    private final int maxSaneEntrySize;

    final ServerConfiguration conf;
    /**
     * Scan entries in a entry log file.
     */
    interface EntryLogScanner {
        /**
         * Tests whether or not the entries belongs to the specified ledger
         * should be processed.
         *
         * @param ledgerId
         *          Ledger ID.
         * @return true if and only the entries of the ledger should be scanned.
         */
        boolean accept(long ledgerId);

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
        void process(long ledgerId, long offset, ByteBuffer entry) throws IOException;
    }

    /**
     * Entry Log Listener.
     */
    interface EntryLogListener {
        /**
         * Rotate a new entry log to write.
         */
        void onRotateEntryLog();
    }

    /**
     * Create an EntryLogger that stores it's log files in the given directories.
     */
    public EntryLogger(ServerConfiguration conf,
            LedgerDirsManager ledgerDirsManager) throws IOException {
        this(conf, ledgerDirsManager, null);
    }

    public EntryLogger(ServerConfiguration conf,
            LedgerDirsManager ledgerDirsManager, EntryLogListener listener)
                    throws IOException {
        //We reserve 500 bytes as overhead for the protocol.  This is not 100% accurate
        // but the protocol varies so an exact value is difficult to determine
        this.maxSaneEntrySize = conf.getNettyMaxFrameSizeBytes() - 500;
        this.ledgerDirsManager = ledgerDirsManager;
        this.conf = conf;
        numberOfActiveEntryLogsPerLedgerDir = conf.getNumberOfActiveEntryLogsPerLedgerDir();
        if (numberOfActiveEntryLogsPerLedgerDir == 0) {
            totalNumberOfActiveEntryLogs = 1;
        } else {
            totalNumberOfActiveEntryLogs = numberOfActiveEntryLogsPerLedgerDir * conf.getLedgerDirs().length;
        }
        
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
        logfileHeader.put("BKLO".getBytes(UTF_8));
        logfileHeader.putInt(HEADER_CURRENT_VERSION);

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
        this.bytesWrittenSinceLastFlush = new AtomicLongArray(totalNumberOfActiveEntryLogs);
        this.locksForEntryLogs = new ReentrantLock[totalNumberOfActiveEntryLogs];
        this.entryLoggerAllocator = new EntryLoggerAllocator(logId);
        this.entryLogManager = new EntryLogManager(conf);
        initialize();
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
    private final ThreadLocal<Map<Long, BufferedReadChannel>> logid2Channel =
            new ThreadLocal<Map<Long, BufferedReadChannel>>() {
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
    private final ConcurrentMap<Long, FileChannel> logid2FileChannel = new ConcurrentHashMap<Long, FileChannel>();

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

    protected void initialize() throws IOException {
        boolean allDisksFull = !ledgerDirsManager.hasWritableLedgerDirs();
        for (int i = 0; i < totalNumberOfActiveEntryLogs; i++) {
            locksForEntryLogs[i] = new ReentrantLock();
            createNewLog(i, entryLogManager.isLedgerDirOfThisSlotFull(i), allDisksFull);
        }
    }

    void rollLogsIfEntryLogLimitReached() throws IOException {
        for (int slot = 0; slot < totalNumberOfActiveEntryLogs; slot++) {
            Lock thisSlotLock = locksForEntryLogs[slot];
            thisSlotLock.lock();
            try {
                if (reachEntryLogLimit(slot, 0L)) {
                    LOG.info("Rolling entry logger since it reached size limitation");
                    createNewLog(slot, false, false);
                }
            } finally {
                thisSlotLock.unlock();
            }
        }
    }
    
    /**
     * Creates a new log file.
     */
    void createNewLog(int slot, boolean diskFull, boolean allDisksFull) throws IOException {
        Lock thisSlotLock = locksForEntryLogs[slot];
        thisSlotLock.lock();
        try {
            BufferedLogChannel logChannel = entryLogManager.getCurrentLogOfSlot(slot); 
            // first tried to create a new log channel. add current log channel to ToFlush list only when
            // there is a new log channel. it would prevent that a log channel is referenced by both
            // *logChannel* and *ToFlush* list.
            if (null != logChannel) {

                // flush the internal buffer back to filesystem but not sync disk
                logChannel.flush(false);

                // Append ledgers map at the end of entry log
                appendLedgersMap(slot);

                BufferedLogChannel newLogChannel = entryLoggerAllocator.createNewLog(slot, diskFull, allDisksFull);
                bytesWrittenSinceLastFlush.set(slot, 0L);
                if (totalNumberOfActiveEntryLogs == 1) {
                    if (newLogChannel == null) {
                        entryLogManager.setCurrentLogInSlot(slot, null, null);
                    } else {
                        entryLogManager.setCurrentLogInSlot(slot, newLogChannel.getFile().getParentFile(),
                                newLogChannel);
                    }
                } else {
                    entryLogManager.setCurrentLogInSlot(slot, newLogChannel);
                }
                LOG.info("Flushing entry logger {} back to filesystem, pending for syncing entry loggers : {}.",
                        logChannel.getLogId(), entryLogManager.getCopyOfLogChannelsToFlush());
                if (totalNumberOfActiveEntryLogs == 1) {
                    for (EntryLogListener listener : listeners) {
                        listener.onRotateEntryLog();
                    }
                }                
            } else {
                entryLogManager.setCurrentLogInSlot(slot, entryLoggerAllocator.createNewLog(slot, diskFull, allDisksFull));                
            }
        } finally {
            thisSlotLock.unlock();
        }
    }

    /**
     * Append the ledger map at the end of the entry log.
     * Updates the entry log file header with the offset and size of the map.
     */
    private void appendLedgersMap(int slot) throws IOException {
        Lock thisSlotLock = locksForEntryLogs[slot];
        thisSlotLock.lock();
        try {
            BufferedLogChannel entryLogChannel = entryLogManager.getCurrentLogOfSlot(slot);

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
                    long ledgerId = entry.getKey();
                    long size = entry.getValue();

                    serializedMap.putLong(ledgerId);
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
            thisSlotLock.unlock();
        }
    }

    /**
     * An allocator pre-allocates entry log files.
     */
    class EntryLoggerAllocator {

        long preallocatedLogId;
        Future<BufferedLogChannel>[] preallocation = null;
        ExecutorService allocatorExecutor;

        @SuppressWarnings("unchecked")
        EntryLoggerAllocator(long logId) {
            preallocatedLogId = logId;
            allocatorExecutor = Executors.newSingleThreadExecutor();
            preallocation = (Future<BufferedLogChannel>[]) new Future[totalNumberOfActiveEntryLogs];
        }
        
        synchronized long getPreallocatedLogId(){
            return preallocatedLogId;
        }

        synchronized BufferedLogChannel createNewLog(int slot, boolean diskFull, boolean allDisksFull) throws IOException {
            BufferedLogChannel bc;
            if (!entryLogPreAllocationEnabled || null == preallocation[slot]) {
                if ((totalNumberOfActiveEntryLogs == 1) || (allDisksFull) || (!diskFull)) {
                    // initialization time to create a new log
                    bc = allocateNewLog(slot, diskFull, allDisksFull);
                } else {
                    return null;
                }
            } else {
                // has a preallocated entry log
                try {
                    bc = preallocation[slot].get();
                    if ((totalNumberOfActiveEntryLogs == 1)) {
                        if(!allDisksFull){
                            File ledgerDir = bc.getFile().getParentFile();
                            /*
                             * if the ledgerdir of the preallocated entrylog is
                             * full then move that entryLog to
                             * logChannelsToFlush list and return null
                             */
                            if(ledgerDirsManager.isDirFull(ledgerDir)){
                                entryLogManager.addToLogChannelsToFlush(bc);
                                preallocation[slot] = null;
                                return null;
                            }
                        }
                    }
                    else if (diskFull && (!allDisksFull)) {
                        entryLogManager.addToLogChannelsToFlush(bc);
                        preallocation[slot] = null;
                        return null;
                    }
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
                }
            }
            if (entryLogPreAllocationEnabled) {
                preallocation[slot] = allocatorExecutor.submit(new Callable<BufferedLogChannel>() {
                    @Override
                    public BufferedLogChannel call() throws IOException {
                        return allocateNewLog(slot, diskFull, allDisksFull);
                    }
                });
            }
            LOG.info("Created new entry logger {}.", bc.getLogId());
            return bc;
        }

        /**
         * Allocate a new log file.
         */
        synchronized BufferedLogChannel allocateNewLog(int slot, boolean diskFull, boolean allDisksFull) throws IOException {
            File dirForNextEntryLog;
            if (totalNumberOfActiveEntryLogs == 1) {
                List<File> list;
                if (allDisksFull) {
                    list = ledgerDirsManager.getWritableLedgerDirsForNewLog();
                } else {
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
                }
                Collections.shuffle(list);
                dirForNextEntryLog = list.get(0);
            } else {
                // if we are having multipleentrylogs, then slot is fixed to the
                // LedgerDir
                dirForNextEntryLog = entryLogManager.getLedgerDirOfSlot(slot);
            }

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
                    conf.getReadBufferBytes(), preallocatedLogId);
            logChannel.write((ByteBuffer) logfileHeader.clear());

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
        } catch (NumberFormatException e) {
            return INVALID_LID;
        } finally {
            try {
                br.close();
            } catch (IOException e) {
            }
        }
    }

    class EntryLogManager {
        /*
         * EntryLogManager instance is guarded by this lock. All write and read operations should be guarded.
         * 
         * One downside of StampedLock is it is not reentrant. Note from API doc - "They are not reentrant, so locked
         * bodies should not call other unknown methods that may try to re-acquire locks (although you may pass a stamp
         * to other methods that can use or convert it)"
         */
        private StampedLock stampedReadWriteLock; 
        private final Slot[] slots;
        private final List<BufferedLogChannel> logChannelsToFlush;
        private Cache<Long, Integer> ledgerIdEntryLogMap;
        private int prevSlot;

        class Slot {
            int slotNum;
            File ledgerDir;
            BufferedLogChannel currentLogChannel;

            Slot(int slotNum, File ledgerDir) {
                this.slotNum = slotNum;
                this.ledgerDir = ledgerDir;
            }

            int getSlotNum() {
                return slotNum;
            }

            File getLedgerDir() {
                return ledgerDir;
            }

            void setLedgerDir(File ledgerDir) {
                // this method should be called only in the case of
                // totalnumberofActiveEntryLogs = 1
                this.ledgerDir = ledgerDir;
            }

            BufferedLogChannel getCurrentLogChannel() {
                return currentLogChannel;
            }

            void setCurrentLogChannel(BufferedLogChannel currentLogChannel) {
                this.currentLogChannel = currentLogChannel;
            }

            boolean isLedgerDirFull() {
                if (ledgerDir == null) {
                    return false;
                }
                return ledgerDirsManager.isDirFull(ledgerDir);
            }
        }

        EntryLogManager(ServerConfiguration conf) throws IOException {
            stampedReadWriteLock = new StampedLock();
            logChannelsToFlush = new ArrayList<BufferedLogChannel>();
            slots = new Slot[totalNumberOfActiveEntryLogs];
            // we are not removing entries from the map explicitly and hence we
            // are relying on timebased eviction policy
            ledgerIdEntryLogMap = CacheBuilder.newBuilder()
                    .expireAfterAccess(ENTRYLOGMAP_ACCESS_EXPIRYTIME_INMINUTES, TimeUnit.MINUTES).build();
            prevSlot = -1;
            initializeSlots();
        }

        private void initializeSlots() throws IOException {
            File[] ledgerDirs = conf.getLedgerDirs();
            for (int i = 0; i < slots.length; i++) {
                int ledgerDirIndex = i % ledgerDirs.length;
                File ledgerDir = Bookie.getCurrentDirectory(ledgerDirs[ledgerDirIndex]);
                slots[i] = new Slot(i, ledgerDir);
            }
        }

        BufferedLogChannel getCurrentLogOfSlot(int slot) {
            long stamp = stampedReadWriteLock.tryOptimisticRead();
            BufferedLogChannel currentLogOfSlot = slots[slot].getCurrentLogChannel();
            if (!stampedReadWriteLock.validate(stamp)) {
                stamp = stampedReadWriteLock.readLock();
                try {
                    currentLogOfSlot = slots[slot].getCurrentLogChannel();
                } finally {
                    stampedReadWriteLock.unlockRead(stamp);
                }
            }
            return currentLogOfSlot;
        }
        
        File getLedgerDirOfSlot(int slot) {
            long stamp = stampedReadWriteLock.tryOptimisticRead();
            File ledgerDirOfSlot = slots[slot].getLedgerDir();
            if (!stampedReadWriteLock.validate(stamp)) {
                stamp = stampedReadWriteLock.readLock();
                try {
                    ledgerDirOfSlot = slots[slot].getLedgerDir();
                } finally {
                    stampedReadWriteLock.unlockRead(stamp);
                }
            }
            return ledgerDirOfSlot;
        }

        BufferedLogChannel getCurrentLogIfPresent(long entryLogId) {
            long stamp = stampedReadWriteLock.tryOptimisticRead();
            BufferedLogChannel logChannel = getCurrentLogIfPresentWithoutGuard(entryLogId);
            if (!stampedReadWriteLock.validate(stamp)) {
                stamp = stampedReadWriteLock.readLock();
                try {
                    logChannel = getCurrentLogIfPresentWithoutGuard(entryLogId);
                } finally {
                    stampedReadWriteLock.unlockRead(stamp);
                }
            }
            return logChannel;
        }

        BufferedLogChannel getCurrentLogIfPresentWithoutGuard(long entryLogId) {
            for (Slot slot : slots) {
                BufferedLogChannel logChannel = slot.getCurrentLogChannel();
                if (logChannel == null) {
                    continue;
                }
                if (logChannel.logId == entryLogId) {
                    return logChannel;
                }
            }
            return null;
        }
        
        /*
         * gets the slotId for the ledger if it is already present. If there is
         * no mapping for this ledger in the map, it will assign next slot to
         * this ledger and return the slot number.
         */
        Integer getCurrentLogSlotForLedgerId(long ledgerId) throws IOException {
            long stamp = stampedReadWriteLock.tryOptimisticRead();
            Integer slot = ledgerIdEntryLogMap.getIfPresent(ledgerId);
            if (!stampedReadWriteLock.validate(stamp)) {
                stamp = stampedReadWriteLock.readLock();
                try {
                    slot = ledgerIdEntryLogMap.getIfPresent(ledgerId);
                } finally {
                    stampedReadWriteLock.unlockRead(stamp);
                }
            }
            if (slot != null) {
                return slot;
            }
            
            stamp = stampedReadWriteLock.writeLock();
            try {
                slot = ledgerIdEntryLogMap.getIfPresent(ledgerId);
                if (slot == null) {
                    /*
                     * here we are already holding writelock, so we are calling unguarded version of getNextSlot. This
                     * is required because StampedLock is non-reentrantlock
                     */
                    ledgerIdEntryLogMap.put(ledgerId, getNextSlotWithoutGuard());
                }
                return ledgerIdEntryLogMap.getIfPresent(ledgerId);
            } finally {
                stampedReadWriteLock.unlockWrite(stamp);
            }
        }
        
        /*
         * update the slot num of the ledgerId to the next slot
         */
        Integer updateSlotForLedgerId(long ledgerId) throws IOException {
            long stamp = stampedReadWriteLock.writeLock();
            try {
                Integer slot = ledgerIdEntryLogMap.getIfPresent(ledgerId);
                if (slot == null) {
                    LOG.error(
                            "We are trying to update the slot of the ledgerid, but there is no mapping for this ledgerid");
                    throw new IOException(
                            "We are trying to update the slot of the ledgerid, but there is no mapping for this ledgerid");
                }
                /*
                 * here we are already holding writelock, so we are calling unguarded version of getNextSlot. This
                 * is required because StampedLock is non-reentrantlock
                 */
                ledgerIdEntryLogMap.put(ledgerId, getNextSlotWithoutGuard());
                return ledgerIdEntryLogMap.getIfPresent(ledgerId);
            } finally {
                stampedReadWriteLock.unlockWrite(stamp);
            }
        }
     
        /*
         * here it round-robins slots and returns the next available slot, for
         * which ledgerDir is not full.
         */
        int getNextSlot() throws IOException {
            long stamp = stampedReadWriteLock.writeLock();
            try {
                return getNextSlotWithoutGuard();
            } finally {
                stampedReadWriteLock.unlockWrite(stamp);
            }
        }
        
        /*
         * here it round-robins slots and returns the next available slot, for which ledgerDir is not full.
         * 
         * This method doesnt acquires writelock, so the caller of this method should be holding writelock of
         * stampedReadWriteLock
         */
        int getNextSlotWithoutGuard() throws IOException {
            int nextSlot = -1;
            for (int i = 1; i <= totalNumberOfActiveEntryLogs; i++) {
                nextSlot = (prevSlot + i) % totalNumberOfActiveEntryLogs;
                if (!slots[nextSlot].isLedgerDirFull()) {
                    break;
                }
            }
            prevSlot = nextSlot;
            return nextSlot;
        }
        
        boolean isLedgerDirOfThisSlotFull(int slot) {
            long stamp = stampedReadWriteLock.tryOptimisticRead();
            boolean isLedgerDirFull = slots[slot].isLedgerDirFull();
            if (!stampedReadWriteLock.validate(stamp)) {
                stamp = stampedReadWriteLock.readLock();
                try {
                    isLedgerDirFull = slots[slot].isLedgerDirFull();
                } finally {
                    stampedReadWriteLock.unlockRead(stamp);
                }
            }
            return isLedgerDirFull;
        }

        /*
         * when a new logChannel is set for this slot, then the existing
         * logChannel of this slot will be added to logChannelsToFlush list.
         */
        void setCurrentLogInSlot(int slot, BufferedLogChannel logChannel) {
            long stamp = stampedReadWriteLock.writeLock();
            try {
                BufferedLogChannel hasToRotateLogChannel = slots[slot].getCurrentLogChannel();
                slots[slot].setCurrentLogChannel(logChannel);
                if (hasToRotateLogChannel != null) {
                    logChannelsToFlush.add(hasToRotateLogChannel);
                }
            } finally {
                stampedReadWriteLock.unlockWrite(stamp);
            }
        }

        /*
         * when a new logChannel is set for this slot, then the existing
         * logChannel of this slot will be added to logChannelsToFlush list.
         * 
         * This setter called when there is only one slot, in that case
         * ledgerDir keeps changing hence ledgerDir is also an argument for this
         * function.
         */
        void setCurrentLogInSlot(int slot, File ledgerDir, BufferedLogChannel logChannel) {
            long stamp = stampedReadWriteLock.writeLock();
            try {
                BufferedLogChannel hasToRotateLogChannel = slots[slot].getCurrentLogChannel();
                slots[slot].setLedgerDir(ledgerDir);
                slots[slot].setCurrentLogChannel(logChannel);
                if (hasToRotateLogChannel != null) {
                    logChannelsToFlush.add(hasToRotateLogChannel);
                }
            } finally {
                stampedReadWriteLock.unlockWrite(stamp);
            }
        }

        List<BufferedLogChannel> getCopyOfLogChannelsToFlush() {
            long stamp = stampedReadWriteLock.tryOptimisticRead();
            List<BufferedLogChannel> copyOfLogChannelsToFlush = new ArrayList<BufferedLogChannel>(
                    logChannelsToFlush);
            if (!stampedReadWriteLock.validate(stamp)) {
                stamp = stampedReadWriteLock.readLock();
                try {
                    copyOfLogChannelsToFlush = new ArrayList<BufferedLogChannel>(
                            logChannelsToFlush);
                } finally {
                    stampedReadWriteLock.unlockRead(stamp);
                }
            }
            return copyOfLogChannelsToFlush;
        }

        List<BufferedLogChannel> getCopyOfCurrentLogs() {
            long stamp = stampedReadWriteLock.tryOptimisticRead();
            List<BufferedLogChannel> copyOfCurrentLogs = getCopyOfCurrentLogsWithoutGuard();
            if (!stampedReadWriteLock.validate(stamp)) {
                stamp = stampedReadWriteLock.readLock();
                try {
                    copyOfCurrentLogs = getCopyOfCurrentLogsWithoutGuard();
                } finally {
                    stampedReadWriteLock.unlockRead(stamp);
                }
            }
            return copyOfCurrentLogs;
        }

        List<BufferedLogChannel> getCopyOfCurrentLogsWithoutGuard() {
            List<BufferedLogChannel> copyOfCurrentLogs = new ArrayList<BufferedLogChannel>();
            for (Slot slot : slots) {
                BufferedLogChannel logChannel = slot.getCurrentLogChannel();
                if (logChannel != null) {
                    copyOfCurrentLogs.add(logChannel);
                }
            }
            return copyOfCurrentLogs;
        }
        
        void addToLogChannelsToFlush(BufferedLogChannel logChannelToAdd) {
            long stamp = stampedReadWriteLock.writeLock();
            try {
                logChannelsToFlush.add(logChannelToAdd);
            } finally {
                stampedReadWriteLock.unlockWrite(stamp);
            }
        }

        void removeFromLogChannelsToFlush(BufferedLogChannel logChannelToRemove) {
            long stamp = stampedReadWriteLock.writeLock();
            try {
                logChannelsToFlush.remove(logChannelToRemove);
            } finally {
                stampedReadWriteLock.unlockWrite(stamp);
            }
        }

        boolean isLogChannelsToFlushEmpty() {
            long stamp = stampedReadWriteLock.tryOptimisticRead();
            boolean isLogChannelsToFlushEmpty = logChannelsToFlush.isEmpty();
            if (!stampedReadWriteLock.validate(stamp)) {
                stamp = stampedReadWriteLock.readLock();
                try {
                    isLogChannelsToFlushEmpty = logChannelsToFlush.isEmpty();
                } finally {
                    stampedReadWriteLock.unlockRead(stamp);
                }
            }
            return isLogChannelsToFlushEmpty;
        }
    }
    
    /**
     * Flushes all rotated log channels. After log channels are flushed,
     * move leastUnflushedLogId ptr to current logId.
     */
    void checkpoint() throws IOException {
        flushRotatedLogs();
    }

    void flushRotatedLogs() throws IOException {
        List<BufferedLogChannel> channels = null;
        synchronized (this) {
            channels = entryLogManager.getCopyOfLogChannelsToFlush();
        }
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
            entryLogManager.removeFromLogChannelsToFlush(channel);
            LOG.info("Synced entry logger {} to disk.", channel.getLogId());
        }
    }

    void flush() throws IOException {
        flushRotatedLogs();
        flushCurrentLogs();
        /*
         * if numberOfActiveLogs is 1 then we checkpoint on rotation of currentLog.
         * so this is not required. But if numberOfActiveEntryLogs is > 1, we are 
         * checkpointing for every 'flushinterval' period.
         */
        if (totalNumberOfActiveEntryLogs > 1) {
            /*
             * this is required because, in between flushRotatedLogs and
             * flushCurrentLogs, an activeEntryLog could have been rotated and
             * added to rotatedLogs.
             */
            if (!entryLogManager.isLogChannelsToFlushEmpty()) {
                flushRotatedLogs();
            }
        }
    }

    void flushCurrentLogs() throws IOException {
        for (int i = 0; i < locksForEntryLogs.length; i++) {
            /**
             * flushCurrentLogs method is called during checkpoint,
             * so metadata of the file also should be force written.
             */
            flushCurrentLog(i, true);
        }
    }

    void flushCurrentLog(int slot, boolean forceMetadata) throws IOException {
        BufferedLogChannel logChannel = entryLogManager.getCurrentLogOfSlot(slot);
        if (logChannel != null) {
            logChannel.flush(true, forceMetadata);
            // since flushCurrentLog method is not guarded by lock, there is possibility
            // that entry might have been added after flush. So instead of setting 
            // value 0 for bytesWrittenSinceLastFlush of this slot, we are setting
            // the numberofBytesInWriteBuffer of this logChannel.
            bytesWrittenSinceLastFlush.set(slot, logChannel.getNumOfBytesInWriteBuffer());
            if (LOG.isDebugEnabled()) {
                LOG.debug("Flush and sync current entry logger {} slot {}.", logChannel.getLogId(), slot);
            }
        }
    }

    long addEntry(long ledger, ByteBuffer entry) throws IOException {
        return addEntry(ledger, entry, true);
    }

    long addEntry(long ledger, ByteBuffer entry, boolean rollLog) throws IOException {
        Integer slot = entryLogManager.getCurrentLogSlotForLedgerId(ledger);
        Lock thisSlotLock = locksForEntryLogs[slot];
        thisSlotLock.lock();
        /**
         * after acquiring lock for the slot, check needs to be done for
         * checking if the ledger is still assigned to this slot. If it is not
         * assigned to this slot anymore, lock of the new slot should be
         * acquired
         */
        Integer currentSlot = entryLogManager.getCurrentLogSlotForLedgerId(ledger);
        while (!slot.equals(currentSlot)) {
            thisSlotLock.unlock();
            slot = currentSlot;
            thisSlotLock = locksForEntryLogs[currentSlot];
            thisSlotLock.lock();
            currentSlot = entryLogManager.getCurrentLogSlotForLedgerId(ledger);
        }
        try {
            int entrySize = entry.remaining() + 4;
            boolean reachEntryLogLimit = rollLog ? reachEntryLogLimit(slot, entrySize) : readEntryLogHardLimit(slot, entrySize);
            // Create new log if logSizeLimit reached or current disk is full
            boolean diskFull = entryLogManager.isLedgerDirOfThisSlotFull(slot);
            boolean allDisksFull = !ledgerDirsManager.hasWritableLedgerDirs();
            BufferedLogChannel logChannel = entryLogManager.getCurrentLogOfSlot(slot);
            if ((diskFull && (!allDisksFull)) || reachEntryLogLimit || (logChannel == null)) {
                if (doRegularFlushes) {
                    flushCurrentLog(slot, false);
                }
                createNewLog(slot, diskFull, allDisksFull);
            }

            logChannel = entryLogManager.getCurrentLogOfSlot(slot);
            /*
             * if logChannel of the current slot is null, it means this
             * ledgerDir is full, hence the slot of the ledger should be updated
             */
            if (logChannel != null) {
                ByteBuffer buff = ByteBuffer.allocate(4);
                buff.putInt(entry.remaining());
                buff.flip();
                logChannel.write(buff);
                long pos = logChannel.position();
                logChannel.write(entry);
                logChannel.registerWrittenEntry(ledger, entrySize);

                incrementBytesWrittenAndMaybeFlush(slot, 4L + entrySize);

                return (logChannel.getLogId() << 32L) | pos;
            } else {
                entryLogManager.updateSlotForLedgerId(ledger);
            }
        } finally {
            thisSlotLock.unlock();
        }
        return addEntry(ledger, entry, rollLog);
    }

    private void incrementBytesWrittenAndMaybeFlush(int slot, long bytesWritten) throws IOException {
        Lock thisSlotLock = locksForEntryLogs[slot];
        thisSlotLock.lock();
        try {
            if (!doRegularFlushes) {
                return;
            }
            bytesWrittenSinceLastFlush.set(slot, bytesWrittenSinceLastFlush.get(slot) + bytesWritten);
            if (bytesWrittenSinceLastFlush.get(slot) > flushIntervalInBytes) {
                flushCurrentLog(slot, false);
            }
        } finally {
            thisSlotLock.unlock();
        }
    }

    static long logIdForOffset(long offset) {
        return offset >> 32L;
    }

    boolean reachEntryLogLimit(int slot, long size) {
        Lock thisSlotLock = locksForEntryLogs[slot];
        thisSlotLock.lock();
        try {
            BufferedLogChannel logChannel = entryLogManager.getCurrentLogOfSlot(slot);
            if (logChannel == null) {
                return false;
            }
            return logChannel.position() + size > logSizeLimit;
        } finally {
            thisSlotLock.unlock();
        }
    }

    boolean readEntryLogHardLimit(int slot, long size) {
        Lock thisSlotLock = locksForEntryLogs[slot];
        thisSlotLock.lock();
        try {
            BufferedLogChannel logChannel = entryLogManager.getCurrentLogOfSlot(slot);
            if (logChannel == null) {
                return false;
            }
            return logChannel.position() + size > Integer.MAX_VALUE;
        } finally {
            thisSlotLock.unlock();
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
            FileNotFoundException newe = new FileNotFoundException(e.getMessage() + " for " + ledgerId
                    + " with location " + location);
            newe.setStackTrace(e.getStackTrace());
            throw newe;
        }
        if (readFromLogChannel(entryLogId, fc, sizeBuff, pos) != sizeBuff.capacity()) {
            throw new Bookie.NoEntryException("Short read from entrylog " + entryLogId, ledgerId, entryId);
        }
        pos += 4;
        sizeBuff.flip();
        int entrySize = sizeBuff.getInt();
        // entrySize does not include the ledgerId
        if (entrySize > maxSaneEntrySize) {
            LOG.warn("Sanity check failed for entry size of " + entrySize + " at location " + pos + " in "
                    + entryLogId);

        }
        if (entrySize < MIN_SANE_ENTRY_SIZE) {
            LOG.error("Read invalid entry length {}", entrySize);
            throw new IOException("Invalid entry length " + entrySize);
        }
        byte data[] = new byte[entrySize];
        ByteBuffer buff = ByteBuffer.wrap(data);
        int rc = readFromLogChannel(entryLogId, fc, buff, pos);
        if (rc != data.length) {
            // Note that throwing NoEntryException here instead of IOException is not
            // without risk. If all bookies in a quorum throw this same exception
            // the client will assume that it has reached the end of the ledger.
            // However, this may not be the case, as a very specific error condition
            // could have occurred, where the length of the entry was corrupted on all
            // replicas. However, the chance of this happening is very very low, so
            // returning NoEntryException is mostly safe.
            throw new Bookie.NoEntryException("Short read for " + ledgerId + "@"
                                              + entryId + " in " + entryLogId + "@"
                                              + pos + "(" + rc + "!=" + data.length + ")", ledgerId, entryId);
        }
        buff.flip();
        long thisLedgerId = buff.getLong();
        if (thisLedgerId != ledgerId) {
            throw new IOException("problem found in " + entryLogId + "@" + entryId + " at position + " + pos
                    + " entry belongs to " + thisLedgerId + " not " + ledgerId);
        }
        long thisEntryId = buff.getLong();
        if (thisEntryId != entryId) {
            throw new IOException("problem found in " + entryLogId + "@" + entryId + " at position + " + pos
                    + " entry is " + thisEntryId + " not " + entryId);
        }

        return data;
    }

    /**
     * Read the header of an entry log.
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
            File f = new File(d, Long.toHexString(logId) + ".log");
            if (f.exists()) {
                return f;
            }
        }
        throw new FileNotFoundException("No file for log " + Long.toHexString(logId));
    }

    /**
     * Scan entry log.
     *
     * @param entryLogId Entry Log Id
     * @param scanner Entry Log Scanner
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
                throw new IOException("Cannot deserialize ledgers map from ledger " + lid + " -- entryLogId: "
                        + entryLogId);
            }

            long entryId = ledgersMapBuffer.getLong();
            if (entryId != LEDGERS_MAP_ENTRY_ID) {
                throw new IOException("Cannot deserialize ledgers map from ledger " + lid + ":" + entryId
                        + " -- entryLogId: " + entryLogId);
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

        if (LOG.isDebugEnabled()) {
            LOG.debug("Retrieved entry log meta data entryLogId: {}, meta: {}", entryLogId, meta);
        }
        return meta;
    }

    /**
     * Shutdown method to gracefully stop entry logger.
     */
    public void shutdown() {
        // since logChannel is buffered channel, do flush when shutting down
        LOG.info("Stopping EntryLogger");
        List<BufferedLogChannel> copyOfCurrentLogs = entryLogManager.getCopyOfCurrentLogs();
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
