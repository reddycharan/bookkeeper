/**
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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.IOException;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLongArray;

import org.apache.bookkeeper.bookie.EntryLogger.BufferedLogChannel;
import org.apache.bookkeeper.bookie.EntryLogger.EntryLogManager;
import org.apache.bookkeeper.bookie.EntryLogger.EntryLoggerAllocator;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.util.DiskChecker;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EntryLogTest {
    private final static Logger LOG = LoggerFactory.getLogger(EntryLogTest.class);

    final List<File> tempDirs = new ArrayList<File>();

    File createTempDir(String prefix, String suffix) throws IOException {
        File dir = IOUtils.createTempDir(prefix, suffix);
        tempDirs.add(dir);
        return dir;
    }

    @After
    public void tearDown() throws Exception {
        for (File dir : tempDirs) {
            FileUtils.deleteDirectory(dir);
        }
        tempDirs.clear();
    }

    @Test(timeout=60000)
    public void testCorruptEntryLog() throws Exception {
        File tmpDir = createTempDir("bkTest", ".dir");
        File curDir = Bookie.getCurrentDirectory(tmpDir);
        Bookie.checkDirectoryStructure(curDir);

        int gcWaitTime = 1000;
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setGcWaitTime(gcWaitTime);
        conf.setLedgerDirNames(new String[] {tmpDir.toString()});
        Bookie bookie = new Bookie(conf);
        // create some entries
        EntryLogger logger = ((InterleavedLedgerStorage)bookie.ledgerStorage).entryLogger;
        logger.addEntry(1, generateEntry(1, 1).nioBuffer());
        logger.addEntry(3, generateEntry(3, 1).nioBuffer());
        logger.addEntry(2, generateEntry(2, 1).nioBuffer());
        logger.flush();
        // now lets truncate the file to corrupt the last entry, which simulates a partial write
        File f = new File(curDir, "0.log");
        RandomAccessFile raf = new RandomAccessFile(f, "rw");
        raf.setLength(raf.length()-10);
        raf.close();
        // now see which ledgers are in the log
        logger = new EntryLogger(conf, bookie.getLedgerDirsManager());

        EntryLogMetadata meta = logger.getEntryLogMetadata(0L);
        LOG.info("Extracted Meta From Entry Log {}", meta);
        assertNotNull(meta.getLedgersMap().get(1L));
        assertNull(meta.getLedgersMap().get(2L));
        assertNotNull(meta.getLedgersMap().get(3L));
    }

    private ByteBuf generateEntry(long ledger, long entry) {
        byte[] data = ("ledger-" + ledger + "-" + entry).getBytes();
        ByteBuf bb = Unpooled.buffer(8 + 8 + data.length);
        bb.writeLong(ledger);
        bb.writeLong(entry);
        bb.writeBytes(data);
        return bb;
    }

    @Test(timeout=60000)
    public void testMissingLogId() throws Exception {
        File tmpDir = createTempDir("entryLogTest", ".dir");
        File curDir = Bookie.getCurrentDirectory(tmpDir);
        Bookie.checkDirectoryStructure(curDir);

        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setLedgerDirNames(new String[] {tmpDir.toString()});
        Bookie bookie = new Bookie(conf);
        // create some entries
        int numLogs = 3;
        int numEntries = 10;
        long[][] positions = new long[2*numLogs][];
        for (int i=0; i<numLogs; i++) {
            positions[i] = new long[numEntries];

            EntryLogger logger = new EntryLogger(conf,
                    bookie.getLedgerDirsManager());
            for (int j=0; j<numEntries; j++) {
                positions[i][j] = logger.addEntry(i, generateEntry(i, j).nioBuffer());
            }
            logger.flush();
        }
        // delete last log id
        File lastLogId = new File(curDir, "lastId");
        lastLogId.delete();

        // write another entries
        for (int i=numLogs; i<2*numLogs; i++) {
            positions[i] = new long[numEntries];

            EntryLogger logger = new EntryLogger(conf,
                    bookie.getLedgerDirsManager());
            for (int j=0; j<numEntries; j++) {
                positions[i][j] = logger.addEntry(i, generateEntry(i, j).nioBuffer());
            }
            logger.flush();
        }

        EntryLogger newLogger = new EntryLogger(conf,
                bookie.getLedgerDirsManager());
        for (int i=0; i<(2*numLogs+1); i++) {
            File logFile = new File(curDir, Long.toHexString(i) + ".log");
            assertTrue(logFile.exists());
        }
        for (int i=0; i<2*numLogs; i++) {
            for (int j=0; j<numEntries; j++) {
                String expectedValue = "ledger-" + i + "-" + j;
                byte[] value = newLogger.readEntry(i, j, positions[i][j]);
                ByteBuffer buf = ByteBuffer.wrap(value);
                long ledgerId = buf.getLong();
                long entryId = buf.getLong();
                byte[] data = new byte[buf.remaining()];
                buf.get(data);
                assertEquals(i, ledgerId);
                assertEquals(j, entryId);
                assertEquals(expectedValue, new String(data));
            }
        }
    }

    @Test(timeout=60000)
    /** Test that EntryLogger Should fail with FNFE, if entry logger directories does not exist*/
    public void testEntryLoggerShouldThrowFNFEIfDirectoriesDoesNotExist()
            throws Exception {
        File tmpDir = createTempDir("bkTest", ".dir");
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setLedgerDirNames(new String[] { tmpDir.toString() });
        EntryLogger entryLogger = null;
        try {
            entryLogger = new EntryLogger(conf, new LedgerDirsManager(conf, conf.getLedgerDirs(),
                    new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold())));
            fail("Expecting FileNotFoundException");
        } catch (FileNotFoundException e) {
            assertEquals("Entry log directory does not exist", e
                    .getLocalizedMessage());
        } finally {
            if (entryLogger != null) {
                entryLogger.shutdown();
            }
        }
    }

    /**
     * Test to verify the DiskFull during addEntry
     */
    @Test(timeout=60000)
    public void testAddEntryFailureOnDiskFull() throws Exception {
        File ledgerDir1 = createTempDir("bkTest", ".dir");
        File ledgerDir2 = createTempDir("bkTest", ".dir");
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setLedgerDirNames(new String[] { ledgerDir1.getAbsolutePath(),
                ledgerDir2.getAbsolutePath() });
        Bookie bookie = new Bookie(conf);
        EntryLogger entryLogger = new EntryLogger(conf,
                bookie.getLedgerDirsManager());
        InterleavedLedgerStorage ledgerStorage = ((InterleavedLedgerStorage) bookie.ledgerStorage);
        ledgerStorage.entryLogger = entryLogger;
        // Create ledgers
        ledgerStorage.setMasterKey(1, "key".getBytes());
        ledgerStorage.setMasterKey(2, "key".getBytes());
        ledgerStorage.setMasterKey(3, "key".getBytes());
        // Add entries
        ledgerStorage.addEntry(generateEntry(1, 1));
        ledgerStorage.addEntry(generateEntry(2, 1));
        // Add entry with disk full failure simulation
        
        bookie.getLedgerDirsManager()
                .addToFilledDirs(entryLogger.entryLogManager.getCurrentLogOfSlot(0).getFile().getParentFile());
        ledgerStorage.addEntry(generateEntry(3, 1));
        // Verify written entries
        Assert.assertTrue(0 == generateEntry(1, 1).compareTo(ledgerStorage.getEntry(1, 1)));
        Assert.assertTrue(0 == generateEntry(2, 1).compareTo(ledgerStorage.getEntry(2, 1)));
        Assert.assertTrue(0 == generateEntry(3, 1).compareTo(ledgerStorage.getEntry(3, 1)));
    }

    /**
     * Explicitely try to recover using the ledgers map index at the end of the entry log
     */
    @Test(timeout=60000)
    public void testRecoverFromLedgersMap() throws Exception {
        File tmpDir = createTempDir("bkTest", ".dir");
        File curDir = Bookie.getCurrentDirectory(tmpDir);
        Bookie.checkDirectoryStructure(curDir);

        int gcWaitTime = 1000;
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setGcWaitTime(gcWaitTime);
        conf.setLedgerDirNames(new String[] {tmpDir.toString()});
        Bookie bookie = new Bookie(conf);

        // create some entries
        EntryLogger logger = ((InterleavedLedgerStorage)bookie.ledgerStorage).entryLogger;
        logger.addEntry(1, generateEntry(1, 1).nioBuffer());
        logger.addEntry(3, generateEntry(3, 1).nioBuffer());
        logger.addEntry(2, generateEntry(2, 1).nioBuffer());
        logger.addEntry(1, generateEntry(1, 2).nioBuffer());
        logger.createNewLog(0, false, false);
        logger.flushRotatedLogs();

        EntryLogMetadata meta = logger.extractEntryLogMetadataFromIndex(0L);
        LOG.info("Extracted Meta From Entry Log {}", meta);
        assertEquals(60, meta.getLedgersMap().get(1L).longValue());
        assertEquals(30, meta.getLedgersMap().get(2L).longValue());
        assertEquals(30, meta.getLedgersMap().get(3L).longValue());
        assertNull(meta.getLedgersMap().get(4L));
        assertEquals(120, meta.getTotalSize());
        assertEquals(120, meta.getRemainingSize());
    }

    /**
     * Explicitely try to recover using the ledgers map index at the end of the entry log
     */
    @Test(timeout = 60000)
    public void testRecoverFromLedgersMapOnV0EntryLog() throws Exception {
        File tmpDir = createTempDir("bkTest", ".dir");
        File curDir = Bookie.getCurrentDirectory(tmpDir);
        Bookie.checkDirectoryStructure(curDir);

        int gcWaitTime = 1000;
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setGcWaitTime(gcWaitTime);
        conf.setLedgerDirNames(new String[] { tmpDir.toString() });
        Bookie bookie = new Bookie(conf);

        // create some entries
        EntryLogger logger = ((InterleavedLedgerStorage) bookie.ledgerStorage).entryLogger;
        logger.addEntry(1, generateEntry(1, 1).nioBuffer());
        logger.addEntry(3, generateEntry(3, 1).nioBuffer());
        logger.addEntry(2, generateEntry(2, 1).nioBuffer());
        logger.addEntry(1, generateEntry(1, 2).nioBuffer());
        logger.createNewLog(0, false, false);

        // Rewrite the entry log header to be on V0 format
        File f = new File(curDir, "0.log");
        RandomAccessFile raf = new RandomAccessFile(f, "rw");
        raf.seek(EntryLogger.HEADER_VERSION_POSITION);
        // Write zeros to indicate V0 + no ledgers map info
        raf.write(new byte[4 + 8]);
        raf.close();

        // now see which ledgers are in the log
        logger = new EntryLogger(conf, bookie.getLedgerDirsManager());

        try {
            logger.extractEntryLogMetadataFromIndex(0L);
            fail("Should not be possible to recover from ledgers map index");
        } catch (IOException e) {
            // Ok
        }

        // Public method should succeed by falling back to scanning the file
        EntryLogMetadata meta = logger.getEntryLogMetadata(0L);
        LOG.info("Extracted Meta From Entry Log {}", meta);
        assertEquals(60, meta.getLedgersMap().get(1L).longValue());
        assertEquals(30, meta.getLedgersMap().get(2L).longValue());
        assertEquals(30, meta.getLedgersMap().get(3L).longValue());
        assertNull(meta.getLedgersMap().get(4L));
        assertEquals(120, meta.getTotalSize());
        assertEquals(120, meta.getRemainingSize());
    }

    /**
     * Test to verify the leastUnflushedLogId logic in EntryLogsStatus
     */
    @Test(timeout = 60000)
    public void testEntryLoggersRecentEntryLogsStatus() throws Exception {
        File ledgerDir1 = createTempDir("bkTest", ".dir");
        File curDir1 = Bookie.getCurrentDirectory(ledgerDir1);
        Bookie.checkDirectoryStructure(curDir1);

        File ledgerDir2 = createTempDir("bkTest", ".dir");
        File curDir2 = Bookie.getCurrentDirectory(ledgerDir2);
        Bookie.checkDirectoryStructure(curDir2);

        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setLedgerDirNames(new String[] { ledgerDir1.getAbsolutePath(), ledgerDir2.getAbsolutePath() });
        LedgerDirsManager ledgerDirsManager = new LedgerDirsManager(conf, conf.getLedgerDirs(),
                new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold()));

        EntryLogger entryLogger = new EntryLogger(conf, ledgerDirsManager);
        EntryLogger.RecentEntryLogsStatus recentlyCreatedLogsStatus = entryLogger.recentlyCreatedEntryLogsStatus;
        // when EntryLogger is initialized, it creates a new log with entryLogId 0
        Assert.assertEquals("entryLogger's leastUnflushedLogId ", 0L, entryLogger.getLeastUnflushedLogId());
        recentlyCreatedLogsStatus.flushRotatedEntryLog(0L);
        // since we marked entrylog - 0 as rotated, LeastUnflushedLogId would be previous rotatedlog+1
        Assert.assertEquals("entryLogger's leastUnflushedLogId ", 1L, entryLogger.getLeastUnflushedLogId());
        recentlyCreatedLogsStatus.createdEntryLog(1L);
        Assert.assertEquals("entryLogger's leastUnflushedLogId ", 1L, entryLogger.getLeastUnflushedLogId());
        recentlyCreatedLogsStatus.createdEntryLog(2L);
        recentlyCreatedLogsStatus.createdEntryLog(3L);
        recentlyCreatedLogsStatus.createdEntryLog(4L);
        Assert.assertEquals("entryLogger's leastUnflushedLogId ", 1L, entryLogger.getLeastUnflushedLogId());
        recentlyCreatedLogsStatus.flushRotatedEntryLog(1L);
        Assert.assertEquals("entryLogger's leastUnflushedLogId ", 2L, entryLogger.getLeastUnflushedLogId());
        recentlyCreatedLogsStatus.flushRotatedEntryLog(3L);
        // here though we rotated entrylog-3, entrylog-2 is not yet rotated so
        // LeastUnflushedLogId should be still 2
        Assert.assertEquals("entryLogger's leastUnflushedLogId ", 2L, entryLogger.getLeastUnflushedLogId());
        recentlyCreatedLogsStatus.flushRotatedEntryLog(2L);
        // entrylog-3 is already rotated, so leastUnflushedLogId should be 4
        Assert.assertEquals("entryLogger's leastUnflushedLogId ", 4L, entryLogger.getLeastUnflushedLogId());
        recentlyCreatedLogsStatus.flushRotatedEntryLog(4L);
        Assert.assertEquals("entryLogger's leastUnflushedLogId ", 5L, entryLogger.getLeastUnflushedLogId());
        recentlyCreatedLogsStatus.createdEntryLog(5L);
        recentlyCreatedLogsStatus.createdEntryLog(7L);
        recentlyCreatedLogsStatus.createdEntryLog(9L);
        Assert.assertEquals("entryLogger's leastUnflushedLogId ", 5L, entryLogger.getLeastUnflushedLogId());
        recentlyCreatedLogsStatus.flushRotatedEntryLog(5L);
        // since we marked entrylog-5 as rotated, LeastUnflushedLogId would be previous rotatedlog+1
        Assert.assertEquals("entryLogger's leastUnflushedLogId ", 6L, entryLogger.getLeastUnflushedLogId());
        recentlyCreatedLogsStatus.flushRotatedEntryLog(7L);
        Assert.assertEquals("entryLogger's leastUnflushedLogId ", 8L, entryLogger.getLeastUnflushedLogId());
    }
    
    @Test(timeout = 60000)
    public void testEntryLogManager() throws Exception {
        File ledgerDir1 = createTempDir("bkTest", ".dir");
        File curDir1 = Bookie.getCurrentDirectory(ledgerDir1);
        Bookie.checkDirectoryStructure(curDir1);

        File ledgerDir2 = createTempDir("bkTest", ".dir");
        File curDir2 = Bookie.getCurrentDirectory(ledgerDir2);
        Bookie.checkDirectoryStructure(curDir2);

        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setEntryLogFilePreAllocationEnabled(false);
        final int numberOfActiveEntryLogsPerLedgerDir = 3;
        conf.setNumberOfActiveEntryLogsPerLedgerDir(numberOfActiveEntryLogsPerLedgerDir);
        conf.setLedgerDirNames(new String[] { ledgerDir1.getAbsolutePath(), ledgerDir2.getAbsolutePath(), });
        final int totalNumberOfActiveEntryLogs = numberOfActiveEntryLogsPerLedgerDir * conf.getLedgerDirs().length;
        LedgerDirsManager ledgerDirsManager = new LedgerDirsManager(conf, conf.getLedgerDirs(),
                new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold()));

        EntryLogger entryLogger = new EntryLogger(conf, ledgerDirsManager);
        EntryLogManager entryLogManager = entryLogger.entryLogManager;

        List<BufferedLogChannel> copyOfCurrentLogs = entryLogManager.getCopyOfCurrentLogs();
        List<BufferedLogChannel> copyOfLogChannelsToFlush = entryLogManager.getCopyOfLogChannelsToFlush();
        Assert.assertEquals("Number of current active EntryLogs ", totalNumberOfActiveEntryLogs,
                copyOfCurrentLogs.size());
        Assert.assertEquals("Number of Rotated Logs ", 0, copyOfLogChannelsToFlush.size());

        long numOfActiveLedgers = totalNumberOfActiveEntryLogs * 4L;
        for (long lid = 0; lid < numOfActiveLedgers; lid++) {
            /*
             * currently there is no entry for this ledgerid in entryLogManager
             * so when getCurrentLogSlotForLedgerId is called it creates new
             * entry <ledgerid, slotid>. it is round-robin and hence slot for
             * this ledgerid = lid%totalNumberOfActiveEntryLogs.
             * 
             * This is because we disabled entryLogFilePreAllocationEnabled.
             */
            Assert.assertEquals("Slot for ledger: " + lid, (Integer) ((int) (lid % totalNumberOfActiveEntryLogs)),
                    entryLogManager.getCurrentLogSlotForLedgerId(lid));
        }

        for (long lid = 0; lid < numOfActiveLedgers; lid++) {
            /*
             * in the previous iteration entries <ledgerid, slotid> are created
             * for these ledgerids. so it should be able to get this time.
             */
            Assert.assertEquals("Slot for ledger: " + lid, (Integer) ((int) (lid % totalNumberOfActiveEntryLogs)),
                    entryLogManager.getCurrentLogSlotForLedgerId(lid));
        }

        Set<BufferedLogChannel> bufferedLogChannelsSet = new HashSet<BufferedLogChannel>();
        for (int i = 0; i < totalNumberOfActiveEntryLogs; i++) {
            bufferedLogChannelsSet.add(entryLogManager.getCurrentLogOfSlot(i));
        }
        Assert.assertEquals("BufferedLogChannelsSet size ", totalNumberOfActiveEntryLogs,
                bufferedLogChannelsSet.size());

        List<File> ledgerCurDirs = Arrays.asList(Bookie.getCurrentDirectories(conf.getLedgerDirs()));
        Assert.assertEquals("entryLogger's leastUnflushedLogId ", 0L, entryLogger.getLeastUnflushedLogId());
        entryLogger.createNewLog(1, false, false);
        File ledgerCurDirForSlot1 = entryLogManager.getCurrentLogOfSlot(1).getFile().getParentFile();
        Assert.assertTrue(ledgerCurDirs.contains(ledgerCurDirForSlot1));

        Assert.assertEquals("entryLogger's leastUnflushedLogId ", 0L, entryLogger.getLeastUnflushedLogId());
        copyOfCurrentLogs = entryLogManager.getCopyOfCurrentLogs();
        copyOfLogChannelsToFlush = entryLogManager.getCopyOfLogChannelsToFlush();
        Assert.assertEquals("Number of current active EntryLogs ", totalNumberOfActiveEntryLogs,
                copyOfCurrentLogs.size());

        /*
         * there should be one logChannelToFlush, since we called
         * entryLogger.createNewLog(1) earlier
         */
        Assert.assertEquals("Number of Rotated Logs ", 1, copyOfLogChannelsToFlush.size());

        bufferedLogChannelsSet = new HashSet<BufferedLogChannel>();
        for (int i = 0; i < totalNumberOfActiveEntryLogs; i++) {
            bufferedLogChannelsSet
                    .add(entryLogManager.getCurrentLogIfPresent(entryLogManager.getCurrentLogOfSlot(i).getLogId()));
        }
        Assert.assertEquals("BufferedLogChannelsSet size ", totalNumberOfActiveEntryLogs,
                bufferedLogChannelsSet.size());
    }
    
    @Test(timeout = 60000)
    public void testReadAddCallsOfMultipleEntryLogs() throws Exception {
        File ledgerDir1 = createTempDir("bkTest", ".dir");
        File curDir1 = Bookie.getCurrentDirectory(ledgerDir1);
        Bookie.checkDirectoryStructure(curDir1);

        File ledgerDir2 = createTempDir("bkTest", ".dir");
        File curDir2 = Bookie.getCurrentDirectory(ledgerDir2);
        Bookie.checkDirectoryStructure(curDir2);

        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        final int numberOfActiveEntryLogsPerLedgerDir = 5;
        conf.setNumberOfActiveEntryLogsPerLedgerDir(numberOfActiveEntryLogsPerLedgerDir);
        conf.setLedgerDirNames(new String[] { ledgerDir1.getAbsolutePath(), ledgerDir2.getAbsolutePath() });
        conf.setEntryLogFilePreAllocationEnabled(false);
        LedgerDirsManager ledgerDirsManager = new LedgerDirsManager(conf, conf.getLedgerDirs(),
                new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold()));

        EntryLogger entryLogger = new EntryLogger(conf, ledgerDirsManager);

        int totalNumberOfActiveEntryLogs = numberOfActiveEntryLogsPerLedgerDir * conf.getLedgerDirs().length;
        int numOfActiveLedgers = totalNumberOfActiveEntryLogs * 2;
        int numEntries = 10;
        long[][] positions = new long[numOfActiveLedgers][];
        for (int i = 0; i < numOfActiveLedgers; i++) {
            positions[i] = new long[numEntries];
        }

        for (int j = 0; j < numEntries; j++) {
            for (int i = 0; i < numOfActiveLedgers; i++) {
                positions[i][j] = entryLogger.addEntry(i, generateEntry(i, j).nioBuffer());
                long entryLogId = (positions[i][j] >> 32L);
                /**
                 * entryLogManager roundrobins ledgers to activeentrylogs, so
                 * entryLogId should be equal to
                 * ledgerid%totalNumberOfActiveEntryLogs
                 * 
                 * Also here setEntryLogFilePreAllocationEnabled is disabled. So
                 * entryLogIds will be sequential during initialization.
                 */
                Assert.assertEquals("EntryLogId for ledger: " + i, (i % totalNumberOfActiveEntryLogs), entryLogId);
            }
        }
        
        for (int j = 0; j < numEntries; j++) {
            for (int i = 0; i < numOfActiveLedgers; i++) {
                String expectedValue = "ledger-" + i + "-" + j;
                byte[] value = entryLogger.readEntry(i, j, positions[i][j]);
                ByteBuffer buf = ByteBuffer.wrap(value);
                long ledgerId = buf.getLong();
                long entryId = buf.getLong();
                byte[] data = new byte[buf.remaining()];
                buf.get(data);
                assertEquals("LedgerId ", i, ledgerId);
                assertEquals("EntryId ", j, entryId);
                assertEquals("Entry Data ", expectedValue, new String(data));
            }
        }

        for (int i = 0; i < totalNumberOfActiveEntryLogs; i++) {
            entryLogger.createNewLog(i, false, false);
        }
        
        entryLogger.flushRotatedLogs();
        
        // reading after flush of rotatedlogs 
        for (int j = 0; j < numEntries; j++) {
            for (int i = 0; i < numOfActiveLedgers; i++) {
                String expectedValue = "ledger-" + i + "-" + j;
                byte[] value = entryLogger.readEntry(i, j, positions[i][j]);
                ByteBuffer buf = ByteBuffer.wrap(value);
                long ledgerId = buf.getLong();
                long entryId = buf.getLong();
                byte[] data = new byte[buf.remaining()];
                buf.get(data);
                assertEquals("LedgerId ", i, ledgerId);
                assertEquals("EntryId ", j, entryId);
                assertEquals("Entry Data ", expectedValue, new String(data));
            }
        }
    }

    class WriteTask implements Callable<Boolean> {
        int ledgerId;
        int entryId;
        AtomicLongArray positions;
        int indexInPositions;
        EntryLogger entryLogger;

        WriteTask(int ledgerId, int entryId, EntryLogger entryLogger, AtomicLongArray positions, int indexInPositions) {
            this.ledgerId = ledgerId;
            this.entryId = entryId;
            this.entryLogger = entryLogger;
            this.positions = positions;
            this.indexInPositions = indexInPositions;
        }

        @Override
        public Boolean call() {
            try {
                positions.set(indexInPositions, entryLogger.addEntry(ledgerId, generateEntry(ledgerId, entryId).nioBuffer()));
            } catch (IOException e) {
                LOG.error("Got Exception for AddEntry call. LedgerId: " + ledgerId + " entryId: " + entryId, e);
                return false;
            }
            return true;
        }
    }
    
    class ReadTask implements Callable<Boolean> {
        int ledgerId;
        int entryId;
        long position;
        EntryLogger entryLogger;

        ReadTask(int ledgerId, int entryId, long position, EntryLogger entryLogger) {
            this.ledgerId = ledgerId;
            this.entryId = entryId;
            this.position = position;
            this.entryLogger = entryLogger;
        }

        @Override
        public Boolean call() {
            try {
                String expectedValue = "ledger-" + ledgerId + "-" + entryId;
                byte[] value = entryLogger.readEntry(ledgerId, entryId, position);
                ByteBuffer buf = ByteBuffer.wrap(value);
                long actualLedgerId = buf.getLong();
                long actualEntryId = buf.getLong();
                byte[] data = new byte[buf.remaining()];
                buf.get(data);
                if (ledgerId != actualLedgerId) {
                    LOG.error("For ledgerId: {} entryId: {} readRequest, actual ledgerId: {}", ledgerId, entryId,
                            actualLedgerId);
                    return false;
                }
                if (entryId != actualEntryId) {
                    LOG.error("For ledgerId: {} entryId: {} readRequest, actual entryId: {}", ledgerId, entryId,
                            actualEntryId);
                    return false;
                }
                if (!expectedValue.equals(new String(data))) {
                    LOG.error("For ledgerId: {} entryId: {} readRequest, actual Data: {}", ledgerId, entryId,
                            new String(data));
                    return false;
                }
            } catch (IOException e) {
                LOG.error("Got Exception for ReadEntry call. LedgerId: " + ledgerId + " entryId: " + entryId, e);
                return false;
            }
            return true;
        }
    }
    
    class FlushCurrentLogsTask implements Callable<Boolean> {
        EntryLogger entryLogger;

        FlushCurrentLogsTask(EntryLogger entryLogger) {
            this.entryLogger = entryLogger;
        }

        @Override
        public Boolean call() throws Exception {
            try {
                entryLogger.flushCurrentLogs();
                return true;
            } catch (IOException e) {
                LOG.error("Got Exception while trying to flushCurrentLogs");
                return false;
            }
        }
    }
    
    @Test(timeout = 60000)
    public void testConcurrentWriteFlushAndReadCallsOfMultipleEntryLogs() throws Exception {
        File ledgerDir1 = createTempDir("bkTest", ".dir");
        File curDir1 = Bookie.getCurrentDirectory(ledgerDir1);
        Bookie.checkDirectoryStructure(curDir1);

        File ledgerDir2 = createTempDir("bkTest", ".dir");
        File curDir2 = Bookie.getCurrentDirectory(ledgerDir2);
        Bookie.checkDirectoryStructure(curDir2);

        File ledgerDir3 = createTempDir("bkTest", ".dir");
        File curDir3 = Bookie.getCurrentDirectory(ledgerDir3);
        Bookie.checkDirectoryStructure(curDir3);

        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        final int numberOfActiveEntryLogsPerLedgerDir = 5;
        conf.setNumberOfActiveEntryLogsPerLedgerDir(numberOfActiveEntryLogsPerLedgerDir);
        conf.setFlushIntervalInBytes(1000 * 25);
        conf.setLedgerDirNames(new String[] { ledgerDir1.getAbsolutePath(), ledgerDir2.getAbsolutePath(),
                ledgerDir3.getAbsolutePath() });
        final int totalNumberOfActiveEntryLogs = numberOfActiveEntryLogsPerLedgerDir * conf.getLedgerDirs().length;
        LedgerDirsManager ledgerDirsManager = new LedgerDirsManager(conf, conf.getLedgerDirs(),
                new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold()));

        EntryLogger entryLogger = new EntryLogger(conf, ledgerDirsManager);
        int numOfActiveLedgers = totalNumberOfActiveEntryLogs;
        int numEntries = 2000;
        final AtomicLongArray positions = new AtomicLongArray(numOfActiveLedgers * numEntries);
        ExecutorService executor = Executors.newFixedThreadPool(40);
        
        List<Callable<Boolean>> writeAndFlushTasks = new ArrayList<Callable<Boolean>>();
        for (int j = 0; j < numEntries; j++) {
            for (int i = 0; i < numOfActiveLedgers; i++) {
                writeAndFlushTasks.add(new WriteTask(i, j, entryLogger, positions, i * numEntries + j));
            }
        }
        
        // add flushCurrentLogs tasks also
        for (int i = 0; i < writeAndFlushTasks.size() / 100; i++) {
            writeAndFlushTasks.add(i * 100, new FlushCurrentLogsTask(entryLogger));
        }
        
        // invoke all those write and flushcurrentlogs tasks all at once concurrently and set timeout
        // 6 seconds for them to complete
        List<Future<Boolean>> writeAndFlushTasksFutures = executor.invokeAll(writeAndFlushTasks, 6, TimeUnit.SECONDS);
        for (int i = 0; i < writeAndFlushTasks.size(); i++) {
            Future<Boolean> future = writeAndFlushTasksFutures.get(i);
            Callable<Boolean> task = writeAndFlushTasks.get(i);
            if (task instanceof WriteTask) {
                WriteTask writeTask = (WriteTask) task;
                int ledgerId = writeTask.ledgerId;
                int entryId = writeTask.entryId;
                Assert.assertTrue("WriteTask should have been completed successfully ledgerId: " + ledgerId
                        + " entryId: " + entryId, future.isDone() && (!future.isCancelled()));
                Assert.assertTrue(
                        "Position for ledgerId: " + ledgerId + " entryId: " + entryId + " should have been set",
                        future.get());
            } else {
                Assert.assertTrue("FlushTask should have been completed successfully. Index " + i,
                        future.isDone() && (!future.isCancelled()));
                Assert.assertTrue("FlushTask should have been succeded without exception. Index " + i, future.get());
            }
        }

        List<ReadTask> readTasks = new ArrayList<ReadTask>();
        for (int j = 0; j < numEntries; j++) {
            for (int i = 0; i < numOfActiveLedgers; i++) {
                readTasks.add(new ReadTask(i, j, positions.get(i * numEntries + j), entryLogger));
            }
        }
        
        // invoke all those readtasks all at once concurrently and set timeout
        // 6 seconds for them to complete
        List<Future<Boolean>> readTasksFutures = executor.invokeAll(readTasks, 6, TimeUnit.SECONDS);
        for (int i = 0; i < numOfActiveLedgers * numEntries; i++) {
            Future<Boolean> future = readTasksFutures.get(i);
            int ledgerId = readTasks.get(i).ledgerId;
            int entryId = readTasks.get(i).entryId;
            Assert.assertTrue(
                    "ReadTask should have been completed successfully ledgerId: " + ledgerId + " entryId: " + entryId,
                    future.isDone() && (!future.isCancelled()));
            Assert.assertTrue("ReadEntry of ledgerId: " + ledgerId + " entryId: " + entryId
                    + " should have been completed successfully", future.get());
        }

        for (int i = 0; i < totalNumberOfActiveEntryLogs; i++) {
            entryLogger.createNewLog(i, false, false);
        }
        entryLogger.flushRotatedLogs();

        // reading after flush of rotatedlogs
        readTasks = new ArrayList<ReadTask>();
        for (int j = 0; j < numEntries; j++) {
            for (int i = 0; i < numOfActiveLedgers; i++) {
                readTasks.add(new ReadTask(i, j, positions.get(i * numEntries + j), entryLogger));
            }
        }
        readTasksFutures = executor.invokeAll(readTasks, 6, TimeUnit.SECONDS);
        for (int i = 0; i < numOfActiveLedgers * numEntries; i++) {
            Future<Boolean> future = readTasksFutures.get(i);
            int ledgerId = readTasks.get(i).ledgerId;
            int entryId = readTasks.get(i).entryId;
            Assert.assertTrue(
                    "ReadTask should have been completed successfully ledgerId: " + ledgerId + " entryId: " + entryId,
                    future.isDone() && (!future.isCancelled()));
            Assert.assertTrue("ReadEntry of ledgerId: " + ledgerId + " entryId: " + entryId
                    + " should have been completed successfully", future.get());
        }
    }
    
    @Test(timeout = 60000)
    public void testFlushOfMultipleEntryLogs() throws Exception {
        File ledgerDir1 = createTempDir("bkTest", ".dir");
        File curDir1 = Bookie.getCurrentDirectory(ledgerDir1);
        Bookie.checkDirectoryStructure(curDir1);

        File ledgerDir2 = createTempDir("bkTest", ".dir");
        File curDir2 = Bookie.getCurrentDirectory(ledgerDir2);
        Bookie.checkDirectoryStructure(curDir2);

        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        final int numberOfActiveEntryLogsPerLedgerDir = 5;
        conf.setNumberOfActiveEntryLogsPerLedgerDir(numberOfActiveEntryLogsPerLedgerDir);
        conf.setFlushIntervalInBytes(10000000);
        conf.setLedgerDirNames(new String[] { ledgerDir1.getAbsolutePath(), ledgerDir2.getAbsolutePath(), });
        LedgerDirsManager ledgerDirsManager = new LedgerDirsManager(conf, conf.getLedgerDirs(),
                new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold()));
        final int totalNumberOfActiveEntryLogs = numberOfActiveEntryLogsPerLedgerDir * conf.getLedgerDirs().length;
        EntryLogger entryLogger = new EntryLogger(conf, ledgerDirsManager);
        EntryLogManager entryLogManager = entryLogger.entryLogManager;

        int numOfActiveLedgers = totalNumberOfActiveEntryLogs * 2;
        int numEntries = 5;

        for (int j = 0; j < numEntries; j++) {
            for (int i = 0; i < numOfActiveLedgers; i++) {
                entryLogger.addEntry(i, generateEntry(i, j).nioBuffer());
            }
        }
        AtomicLongArray bytesWrittenSinceLastFlush = entryLogger.bytesWrittenSinceLastFlush;
        for (int i = 0; i < totalNumberOfActiveEntryLogs; i++) {
            Assert.assertTrue("bytesWrittenSinceLastFlush should be greater than 0",
                    bytesWrittenSinceLastFlush.get(i) > 0);
        }

        for (int i = 0; i < totalNumberOfActiveEntryLogs; i++) {
            entryLogger.createNewLog(i, false, false);
        }
        
        List<BufferedLogChannel> rotatedLogs = entryLogManager.getCopyOfLogChannelsToFlush();
        Assert.assertEquals("Number of rotated entrylogs", totalNumberOfActiveEntryLogs, rotatedLogs.size());
        Set<BufferedLogChannel> rotatedLogsSet = new HashSet<BufferedLogChannel>();
        for (int i = 0; i < rotatedLogs.size(); i++) {
            rotatedLogsSet.add(rotatedLogs.get(i));
        }
        Assert.assertEquals("Number of rotated entrylogs", totalNumberOfActiveEntryLogs, rotatedLogsSet.size());
        
        /*
         * Since newlog is created for all slots, so they are moved to rotated
         * logs and hence bytesWrittenSinceLastFlush of all the slots should be
         * 0.
         */
        for (int i = 0; i < totalNumberOfActiveEntryLogs; i++) {
            Assert.assertEquals("bytesWrittenSinceLastFlush should be 0", 0L, bytesWrittenSinceLastFlush.get(i));
        }

        for (int j = 0; j < numEntries; j++) {
            for (int i = 30; i < (30 + numOfActiveLedgers); i++) {
                entryLogger.addEntry(i, generateEntry(i, j).nioBuffer());
            }
        }

        for (int i = 0; i < totalNumberOfActiveEntryLogs; i++) {
            Assert.assertTrue("bytesWrittenSinceLastFlush should be greater than 0",
                    bytesWrittenSinceLastFlush.get(i) > 0);
        }

        entryLogger.flush();
        Assert.assertEquals("NUmber of rotated entrylogs", 0, entryLogManager.getCopyOfLogChannelsToFlush().size());
        /*
         * after flush (flushCurrentLogs) bytesWrittenSinceLastFlush should be 0. 
         */
        for (int i = 0; i < totalNumberOfActiveEntryLogs; i++) {
            Assert.assertEquals("bytesWrittenSinceLastFlush should 0", 0L, bytesWrittenSinceLastFlush.get(i));
        }
    }

    /**
     * testcase to validate how the slots are updated for the ledgers as
     * ledgerdirs get filled and later become writable.
     */
    @Test(timeout = 60000)
    public void testEntryLoggerAddEntryWhenLedgerDirsAreFull() throws Exception {
        int numberOfLedgerDirs = 8;
        List<File> ledgerDirs = new ArrayList<File>();
        String[] ledgerDirsPath = new String[numberOfLedgerDirs];
        List<File> curDirs = new ArrayList<File>();

        File ledgerDir;
        File curDir;
        for (int i = 0; i < numberOfLedgerDirs; i++) {
            ledgerDir = createTempDir("bkTest", ".dir").getAbsoluteFile();
            curDir = Bookie.getCurrentDirectory(ledgerDir);
            Bookie.checkDirectoryStructure(curDir);
            ledgerDirs.add(ledgerDir);
            ledgerDirsPath[i] = ledgerDir.getPath();
            curDirs.add(curDir);
        }

        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        // pre-allocation is disabled
        conf.setEntryLogFilePreAllocationEnabled(false);
        final int numberOfActiveEntryLogsPerLedgerDir = 1;
        conf.setNumberOfActiveEntryLogsPerLedgerDir(numberOfActiveEntryLogsPerLedgerDir);
        conf.setLedgerDirNames(ledgerDirsPath);
        final int totalNumberOfActiveEntryLogs = numberOfActiveEntryLogsPerLedgerDir * conf.getLedgerDirs().length;
        LedgerDirsManager ledgerDirsManager = new LedgerDirsManager(conf, conf.getLedgerDirs(),
                new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold()));

        EntryLogger entryLogger = new EntryLogger(conf, ledgerDirsManager);
        EntryLogManager entryLogManager = entryLogger.entryLogManager;
        EntryLoggerAllocator entryLoggerAllocator = entryLogger.entryLoggerAllocator;

        entryLogger.addEntry(0, generateEntry(0, 1).nioBuffer());
        entryLogger.addEntry(1, generateEntry(1, 1).nioBuffer());
        int slotForLedger0 = entryLogManager.getCurrentLogSlotForLedgerId(0);
        int slotForLedger1 = entryLogManager.getCurrentLogSlotForLedgerId(1);
        File ledgerDirForLedger0 = entryLogManager.getLedgerDirOfSlot(slotForLedger0);
        File ledgerDirForLedger1 = entryLogManager.getLedgerDirOfSlot(slotForLedger1);

        /*
         * Allotment of slot to ledgers is round-robin and also while
         * initialization allotment of LedgerDirs to slots is round-robin. So
         * curDirs[0] would be allotted to slot[0] and slot[0] is allotted to
         * ledger-0, similarly for ledger-1.
         */
        Assert.assertEquals("Slot for Ledger 0", 0, slotForLedger0);
        Assert.assertEquals("Slot for Ledger 1", 1, slotForLedger1);
        Assert.assertEquals("LedgerDir for Ledger 0", curDirs.get(0), ledgerDirForLedger0);
        Assert.assertEquals("LedgerDir for Ledger 1", curDirs.get(1), ledgerDirForLedger1);
        Assert.assertEquals("LogChannelsTo flush count", 0,
                entryLogger.entryLogManager.getCopyOfLogChannelsToFlush().size());
        Assert.assertEquals("PreallocatedLogId", totalNumberOfActiveEntryLogs - 1,
                entryLoggerAllocator.getPreallocatedLogId());

        entryLogger.addEntry(0, generateEntry(0, 2).nioBuffer());
        slotForLedger0 = entryLogManager.getCurrentLogSlotForLedgerId(0);
        Assert.assertEquals("Slot for Ledger 0", 0, slotForLedger0);

        /*
         * curDirs[0] is added to filleddirs, so entrylog in slot[0] should be
         * added toflush list and ledger-0 should be assigned to next slot (slot[2])
         * because of addEntry call.
         */
        ledgerDirsManager.addToFilledDirs(curDirs.get(0));
        entryLogger.addEntry(0, generateEntry(0, 3).nioBuffer());

        slotForLedger0 = entryLogManager.getCurrentLogSlotForLedgerId(0);
        ledgerDirForLedger0 = entryLogManager.getLedgerDirOfSlot(slotForLedger0);
        Assert.assertEquals("Slot for Ledger 0", 2, slotForLedger0);
        Assert.assertEquals("LedgerDir for Ledger 0", curDirs.get(2), ledgerDirForLedger0);
        Assert.assertEquals("LogChannels Toflush count", 1,
                entryLogger.entryLogManager.getCopyOfLogChannelsToFlush().size());
        Assert.assertEquals("Current ActiveEntryLogs count", totalNumberOfActiveEntryLogs - 1,
                entryLogger.entryLogManager.getCopyOfCurrentLogs().size());
        Assert.assertEquals("PreallocatedLogId", totalNumberOfActiveEntryLogs - 1,
                entryLoggerAllocator.getPreallocatedLogId());

        entryLogger.addEntry(0, generateEntry(0, 4).nioBuffer());
        slotForLedger0 = entryLogManager.getCurrentLogSlotForLedgerId(0);
        ledgerDirForLedger0 = entryLogManager.getLedgerDirOfSlot(slotForLedger0);
        Assert.assertEquals("Slot for Ledger 0", 2, slotForLedger0);
        Assert.assertEquals("LedgerDir for Ledger 0", curDirs.get(2), ledgerDirForLedger0);

        /*
         * curDirs[1,3,4] are added to filleddirs, so entrylog in slot[1] should be
         * added toflush list and ledger-1 should be assigned to next slot (slot[5])
         * because of addEntry call.
         */
        ledgerDirsManager.addToFilledDirs(curDirs.get(1));
        ledgerDirsManager.addToFilledDirs(curDirs.get(3));
        ledgerDirsManager.addToFilledDirs(curDirs.get(4));
        entryLogger.addEntry(1, generateEntry(1, 2).nioBuffer());
        
        slotForLedger1 = entryLogManager.getCurrentLogSlotForLedgerId(1);
        ledgerDirForLedger1 = entryLogManager.getLedgerDirOfSlot(slotForLedger1);
        Assert.assertEquals("Slot for Ledger 1", 5, slotForLedger1);
        Assert.assertEquals("LedgerDir for Ledger 1", curDirs.get(5), ledgerDirForLedger1);
        Assert.assertEquals("LogChannels Toflush count", 2,
                entryLogger.entryLogManager.getCopyOfLogChannelsToFlush().size());
        Assert.assertEquals("Current ActiveEntryLogs count", totalNumberOfActiveEntryLogs - 2,
                entryLogger.entryLogManager.getCopyOfCurrentLogs().size());
        Assert.assertEquals("PreallocatedLogId", totalNumberOfActiveEntryLogs - 1,
                entryLoggerAllocator.getPreallocatedLogId());

        /*
         * curDirs[2,6,7] are added to filleddirs but curDirs[1] is added to
         * writableDirs, so when entry is added to ledger-0, entrylog in slot[2]
         * should me moved to flush, ledger-0 should be assigned to slot[1],
         * since dirs of next slots - 6,7,0 are filled. But since there is no
         * entrylog in slot[1], it will create a new one and hence
         * PreallocatedLogId is advanced.
         */
        ledgerDirsManager.addToFilledDirs(curDirs.get(2));
        ledgerDirsManager.addToFilledDirs(curDirs.get(6));
        ledgerDirsManager.addToFilledDirs(curDirs.get(7));
        ledgerDirsManager.addToWritableDirs(curDirs.get(1), true);

        entryLogger.addEntry(0, generateEntry(0, 5).nioBuffer());
        slotForLedger0 = entryLogManager.getCurrentLogSlotForLedgerId(0);
        ledgerDirForLedger0 = entryLogManager.getLedgerDirOfSlot(slotForLedger0);
        Assert.assertEquals("Slot for Ledger 0", 1, slotForLedger0);
        Assert.assertEquals("LedgerDir for Ledger 0", curDirs.get(1), ledgerDirForLedger0);
        Assert.assertEquals("LogChannels Toflush count", 3,
                entryLogger.entryLogManager.getCopyOfLogChannelsToFlush().size());
        // +1 because Slot/Dir:1 has opened up
        Assert.assertEquals("Current ActiveEntryLogs count", totalNumberOfActiveEntryLogs - 3 + 1,
                entryLogger.entryLogManager.getCopyOfCurrentLogs().size());
        // since Slot/Dir:1 has opened up new entrylog is created
        Assert.assertEquals("PreallocatedLogId", totalNumberOfActiveEntryLogs,
                entryLoggerAllocator.getPreallocatedLogId());

        /*
         * curDirs[5] is added to filled so ledger-1 will also be assigned to
         * remaining slot - slot[1]
         */
        ledgerDirsManager.addToFilledDirs(curDirs.get(5));
        entryLogger.addEntry(1, generateEntry(1, 3).nioBuffer());
        slotForLedger1 = entryLogManager.getCurrentLogSlotForLedgerId(1);
        ledgerDirForLedger1 = entryLogManager.getLedgerDirOfSlot(slotForLedger1);
        Assert.assertEquals("Slot for Ledger 1", 1, slotForLedger1);
        Assert.assertEquals("LedgerDir for Ledger 1", curDirs.get(1), ledgerDirForLedger1);
        Assert.assertEquals("LogChannels Toflush count", 4,
                entryLogger.entryLogManager.getCopyOfLogChannelsToFlush().size());
        Assert.assertEquals("Current ActiveEntryLogs count", totalNumberOfActiveEntryLogs - 3,
                entryLogger.entryLogManager.getCopyOfCurrentLogs().size());
        Assert.assertEquals("PreallocatedLogId", totalNumberOfActiveEntryLogs,
                entryLoggerAllocator.getPreallocatedLogId());
        
        /*
         * now the only remaining dir - curDirs[1] is also filled, so if allDirs
         * are full then it will continue to use the existing entrylog and no
         * new new log will be created
         */
        ledgerDirsManager.addToFilledDirs(curDirs.get(1));
        entryLogger.addEntry(0, generateEntry(0, 6).nioBuffer());
        entryLogger.addEntry(1, generateEntry(1, 4).nioBuffer());
        slotForLedger0 = entryLogManager.getCurrentLogSlotForLedgerId(0);
        slotForLedger1 = entryLogManager.getCurrentLogSlotForLedgerId(1);
        Assert.assertEquals("Slot for Ledger 0", 1, slotForLedger0);
        Assert.assertEquals("Slot for Ledger 1", 1, slotForLedger1);
        Assert.assertEquals("LogChannels Toflush count", 4,
                entryLogger.entryLogManager.getCopyOfLogChannelsToFlush().size());
        Assert.assertEquals("Current ActiveEntryLogs count", totalNumberOfActiveEntryLogs - 3,
                entryLogger.entryLogManager.getCopyOfCurrentLogs().size());
        Assert.assertEquals("PreallocatedLogId", totalNumberOfActiveEntryLogs,
                entryLoggerAllocator.getPreallocatedLogId());
        
        entryLogger.addEntry(0, generateEntry(0, 7).nioBuffer());
        entryLogger.addEntry(1, generateEntry(1, 5).nioBuffer());
        slotForLedger0 = entryLogManager.getCurrentLogSlotForLedgerId(0);
        slotForLedger1 = entryLogManager.getCurrentLogSlotForLedgerId(1);
        Assert.assertEquals("Slot for Ledger 0", 1, slotForLedger0);
        Assert.assertEquals("Slot for Ledger 1", 1, slotForLedger1);
        Assert.assertEquals("LogChannels Toflush count", 4,
                entryLogger.entryLogManager.getCopyOfLogChannelsToFlush().size());
        Assert.assertEquals("Current ActiveEntryLogs count", totalNumberOfActiveEntryLogs - 3,
                entryLogger.entryLogManager.getCopyOfCurrentLogs().size());
        Assert.assertEquals("PreallocatedLogId", totalNumberOfActiveEntryLogs,
                entryLoggerAllocator.getPreallocatedLogId());
    }
}
