/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.bookkeeper.bookie;

import java.io.File;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;

import org.apache.bookkeeper.bookie.EntryLogger.BufferedLogChannel;
import org.apache.bookkeeper.bookie.EntryLogger.EntryLoggerAllocator;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.util.DiskChecker;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateNewLogTest {
    private static final Logger LOG = LoggerFactory
    .getLogger(CreateNewLogTest.class);
        
    private String[] ledgerDirs; 
    private int numDirs = 100;
    
    @Before
    public void setUp() throws Exception{
        ledgerDirs = new String[numDirs];
        for(int i = 0; i < numDirs; i++){
            File temp = File.createTempFile("bookie", "test");
            temp.delete();
            temp.mkdir();
            File currentTemp = new File(temp.getAbsoluteFile() + "/current");
            currentTemp.mkdir();
            ledgerDirs[i] = temp.getPath();
        }        
    }
    
    @After
    public void tearDown() throws Exception{
        for(int i = 0; i < numDirs; i++){
            File f = new File(ledgerDirs[i]);
            deleteRecursive(f);
        }
    }
    
    private void deleteRecursive(File f) {
        if (f.isDirectory()){
            for (File c : f.listFiles()){
                deleteRecursive(c);
            }
        }
        
        f.delete();
    }
    
    /**
     * Checks if new log file id is verified against all directories.
     * 
     * {@link https://issues.apache.org/jira/browse/BOOKKEEPER-465}
     * 
     * @throws Exception
     */
    @Test(timeout=60000)
    public void testCreateNewLog() throws Exception {
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
                     
        // Creating a new configuration with a number of 
        // ledger directories.
        conf.setLedgerDirNames(ledgerDirs);
        LedgerDirsManager ledgerDirsManager = new LedgerDirsManager(conf, conf.getLedgerDirs(),
                new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold()));
        EntryLogger el = new EntryLogger(conf, ledgerDirsManager);
        
        // Extracted from createNewLog()
        String logFileName = Long.toHexString(1) + ".log";
        File dir = ledgerDirsManager.pickRandomWritableDir();
        LOG.info("Picked this directory: " + dir);
        File newLogFile = new File(dir, logFileName);
        newLogFile.createNewFile();
        
        // Calls createNewLog, and with the number of directories we
        // are using, if it picks one at random it will fail.
        el.createNewLog(0, false, false);
        LOG.info("This is the current log id: " + el.getPreviousAllocatedEntryLogId());
        Assert.assertTrue("Wrong log id", el.getPreviousAllocatedEntryLogId() > 1);
    }

    @Test(timeout=60000)
    public void testCreateNewLogWithNoWritableLedgerDirs() throws Exception {
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();

        // Creating a new configuration with a number of ledger directories.
        conf.setLedgerDirNames(ledgerDirs);
        conf.setIsForceGCAllowWhenNoSpace(true);
        LedgerDirsManager ledgerDirsManager = new LedgerDirsManager(conf, conf.getLedgerDirs(),
                new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold()));
        EntryLogger el = new EntryLogger(conf, ledgerDirsManager);

        // Extracted from createNewLog()
        String logFileName = Long.toHexString(1) + ".log";
        File dir = ledgerDirsManager.pickRandomWritableDir();
        LOG.info("Picked this directory: " + dir);
        File newLogFile = new File(dir, logFileName);
        newLogFile.createNewFile();

        // Now let us move all dirs to filled dirs
        List<File> wDirs = ledgerDirsManager.getWritableLedgerDirs();
        for (File tdir: wDirs) {
            ledgerDirsManager.addToFilledDirs(tdir);
        }

        // Calls createNewLog, and with the number of directories we
        // are using, if it picks one at random it will fail.
        el.createNewLog(0, true, true);
        LOG.info("This is the current log id: " + el.getPreviousAllocatedEntryLogId());
        Assert.assertTrue("Wrong log id", el.getPreviousAllocatedEntryLogId() > 1);
    }

    @Test(timeout = 60000)
    public void testEntryLogPreAllocation() throws Exception {
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        int numberOfActiveEntryLogsPerLedgerDir = 3;

        // Creating a new configuration with a number of ledger directories.
        conf.setLedgerDirNames(ledgerDirs);
        conf.setIsForceGCAllowWhenNoSpace(true);
        // preAllocation is Enabled
        conf.setEntryLogFilePreAllocationEnabled(true);
        conf.setNumberOfActiveEntryLogsPerLedgerDir(numberOfActiveEntryLogsPerLedgerDir);
        LedgerDirsManager ledgerDirsManager = new LedgerDirsManager(conf, conf.getLedgerDirs(),
                new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold()));
        EntryLogger entryLogger = new EntryLogger(conf, ledgerDirsManager);
        EntryLoggerAllocator entryLoggerAllocator = entryLogger.entryLoggerAllocator;
        Thread.sleep(2000);
        
        /*
         * since PreAllocation is enabled for every slot 2 entrylogs will be created
         */
        int expectedPreAllocatedLogIDDuringInitialization = ((numberOfActiveEntryLogsPerLedgerDir
                * conf.getLedgerDirs().length) * 2 - 1);
        Assert.assertEquals("PreallocatedlogId after initialization of Entrylogger",
                expectedPreAllocatedLogIDDuringInitialization, entryLoggerAllocator.getPreallocatedLogId());

        entryLogger.createNewLog(5, false, false);
        Thread.sleep(100);
        Assert.assertEquals("PreallocatedlogId after initialization of Entrylogger",
                expectedPreAllocatedLogIDDuringInitialization + 1, entryLoggerAllocator.getPreallocatedLogId());
        Assert.assertEquals("Number of LogChannels to flush", 1,
                entryLogger.entryLogManager.getCopyOfLogChannelsToFlush().size());

        // create dummy entrylog file with id - (expectedPreAllocatedLogIDDuringInitialization + 2)
        String logFileName = Long.toHexString(expectedPreAllocatedLogIDDuringInitialization + 2) + ".log";
        File dir = ledgerDirsManager.pickRandomWritableDir();
        LOG.info("Picked this directory: " + dir);
        File newLogFile = new File(dir, logFileName);
        newLogFile.createNewFile();

        /*
         * since there is already preexisting entrylog file with id -
         * (expectedPreAllocatedLogIDDuringInitialization + 2), when new
         * entrylog is created it should have
         * (expectedPreAllocatedLogIDDuringInitialization + 3) id
         */
        entryLogger.createNewLog(3, false, false);
        Thread.sleep(100);
        Assert.assertEquals("PreallocatedlogId after initialization of Entrylogger",
                expectedPreAllocatedLogIDDuringInitialization + 3, entryLoggerAllocator.getPreallocatedLogId());
        Assert.assertEquals("Number of LogChannels to flush", 2,
                entryLogger.entryLogManager.getCopyOfLogChannelsToFlush().size());

        /*
         * since we are calling createNewLog with ledgerdir full param, it
         * should not create new entrylog and both the entrylogs (the existing
         * one and the preallocated one) should be moved toflush list.
         */
        entryLogger.createNewLog(4, true, false);
        Thread.sleep(100);
        Assert.assertEquals("PreallocatedlogId", expectedPreAllocatedLogIDDuringInitialization + 3,
                entryLoggerAllocator.getPreallocatedLogId());
        Assert.assertEquals("EntryLog of this slot should be null since dir is full", null,
                entryLogger.entryLogManager.getCurrentLogOfSlot(4));
        Assert.assertEquals("Preallocation Future of this slot should be null since dir is full", null,
                entryLogger.entryLoggerAllocator.preallocation[4]);
        Assert.assertEquals("Number of LogChannels to flush", 4,
                entryLogger.entryLogManager.getCopyOfLogChannelsToFlush().size());

        /*
         * we should be able to get entryLogMetadata from all the active
         * entrylogs and the logs which are moved toflush list. Since no entry
         * is added, all the meta should be empty.
         */
        for (int i = 0; i <= expectedPreAllocatedLogIDDuringInitialization + 3; i++) {
            EntryLogMetadata meta = entryLogger.getEntryLogMetadata(i);
            Assert.assertTrue("EntryLogMetadata should be empty", meta.isEmpty());
            Assert.assertTrue("EntryLog usage should be 0", meta.getTotalSize() == 0);
        }
        
        /*
         * since we are calling createNewLog with allledgerdirs full param, it
         * should create new entrylog and also preallocate new entrylog.
         */
        entryLogger.createNewLog(4, true, true);
        Thread.sleep(100);
        Assert.assertEquals("PreallocatedlogId", expectedPreAllocatedLogIDDuringInitialization + 5,
                entryLoggerAllocator.getPreallocatedLogId());
        Assert.assertNotEquals("EntryLog of this slot should not be null since alldirs are full", null,
                entryLogger.entryLogManager.getCurrentLogOfSlot(4));
        Assert.assertNotEquals("EntryLog of this slot should not be null since alldirs are full", null,
                entryLogger.entryLoggerAllocator.preallocation[4]);
        Assert.assertEquals("Number of LogChannels to flush", 4,
                entryLogger.entryLogManager.getCopyOfLogChannelsToFlush().size());
    }

    @Test(timeout = 60000)
    public void testLogCreationWithNoPreAllocation() throws Exception {
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        int numberOfActiveEntryLogsPerLedgerDir = 3;

        // Creating a new configuration with a number of ledger directories.
        conf.setLedgerDirNames(ledgerDirs);
        conf.setIsForceGCAllowWhenNoSpace(true);
        // preAllocation is not Enabled
        conf.setEntryLogFilePreAllocationEnabled(false);
        conf.setNumberOfActiveEntryLogsPerLedgerDir(numberOfActiveEntryLogsPerLedgerDir);
        LedgerDirsManager ledgerDirsManager = new LedgerDirsManager(conf, conf.getLedgerDirs(),
                new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold()));
        EntryLogger entryLogger = new EntryLogger(conf, ledgerDirsManager);
        EntryLoggerAllocator entryLoggerAllocator = entryLogger.entryLoggerAllocator;
        Thread.sleep(2000);
        int numberOfSlots = (numberOfActiveEntryLogsPerLedgerDir * conf.getLedgerDirs().length);
        int expectedPreAllocatedLogIDDuringInitialization = (numberOfSlots - 1);
        Assert.assertEquals("PreallocatedlogId after initialization of Entrylogger",
                expectedPreAllocatedLogIDDuringInitialization, entryLoggerAllocator.getPreallocatedLogId());

        Future<BufferedLogChannel>[] preallocation = entryLoggerAllocator.preallocation;
        for (int i = 0; i < numberOfSlots; i++) {
            // since preAllocation is not Enabled
            Assert.assertEquals("Preallocation future should not be set for slot " + i, null, preallocation[i]);
        }

        entryLogger.createNewLog(5, false, false);
        Thread.sleep(100);
        Assert.assertEquals("PreallocatedlogId", expectedPreAllocatedLogIDDuringInitialization + 1,
                entryLoggerAllocator.getPreallocatedLogId());
        Assert.assertEquals("Preallocation future should not be set for slot " + 5, null, preallocation[5]);

        // calling with ledgerDir full
        entryLogger.createNewLog(4, true, false);
        Thread.sleep(100);
        Assert.assertEquals("PreallocatedlogId", expectedPreAllocatedLogIDDuringInitialization + 1,
                entryLoggerAllocator.getPreallocatedLogId());
        Assert.assertEquals("EntryLog of this slot should be null since dir is full", null,
                entryLogger.entryLogManager.getCurrentLogOfSlot(4));
        Assert.assertEquals("Preallocation Future of this slot should be null", null,
                entryLogger.entryLoggerAllocator.preallocation[4]);

        // calling with allledgerdirs full
        entryLogger.createNewLog(4, true, true);
        Thread.sleep(100);
        Assert.assertEquals("PreallocatedlogId", expectedPreAllocatedLogIDDuringInitialization + 2,
                entryLoggerAllocator.getPreallocatedLogId());
        Assert.assertNotEquals("EntryLog of this slot should not be null since alldirs are full", null,
                entryLogger.entryLogManager.getCurrentLogOfSlot(4));
        Assert.assertEquals("EntryLog of this slot should be null", null,
                entryLogger.entryLoggerAllocator.preallocation[4]);
    }

    @Test(timeout = 60000)
    public void testEntryLogPreAllocationWithSingleSlot() throws Exception {
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        // set numberOfActiveEntryLogsPerLedgerDir to 0 to have only one slot
        int numberOfActiveEntryLogsPerLedgerDir = 0;

        // Creating a new configuration with a number of ledger directories.
        conf.setLedgerDirNames(ledgerDirs);
        conf.setIsForceGCAllowWhenNoSpace(true);
        // preAllocation is Enabled
        conf.setEntryLogFilePreAllocationEnabled(true);
        conf.setNumberOfActiveEntryLogsPerLedgerDir(numberOfActiveEntryLogsPerLedgerDir);
        LedgerDirsManager ledgerDirsManager = new LedgerDirsManager(conf, conf.getLedgerDirs(),
                new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold()));
        EntryLogger entryLogger = new EntryLogger(conf, ledgerDirsManager);
        EntryLoggerAllocator entryLoggerAllocator = entryLogger.entryLoggerAllocator;
        Thread.sleep(200);
        int expectedPreAllocatedLogIDDuringInitialization = 1;
        Assert.assertEquals("PreallocatedlogId after initialization of Entrylogger",
                expectedPreAllocatedLogIDDuringInitialization, entryLoggerAllocator.getPreallocatedLogId());

        Assert.assertEquals("Next slot should always be 0 is numberOfActiveEntryLogsPerLedgerDir is set to false", 0,
                entryLogger.entryLogManager.getNextSlot());
        Assert.assertEquals("Next slot should always be 0 is numberOfActiveEntryLogsPerLedgerDir is set to false", 0,
                entryLogger.entryLogManager.getNextSlot());
        Set<File> ledgerDirsOfSlot0 = new HashSet<File>();
        ledgerDirsOfSlot0.add(entryLogger.entryLogManager.getLedgerDirOfSlot(0));

        entryLogger.createNewLog(0, false, false);
        Thread.sleep(100);
        Assert.assertEquals("PreallocatedlogId after initialization of Entrylogger",
                expectedPreAllocatedLogIDDuringInitialization + 1, entryLoggerAllocator.getPreallocatedLogId());
        ledgerDirsOfSlot0.add(entryLogger.entryLogManager.getLedgerDirOfSlot(0));

        /*
         * creating newlog with ledgerDir full. Since it is single slot, newlog
         * should be created and also new log should be created for preallocated
         * one. But it would use a dir which is not full.
         */
        entryLogger.createNewLog(0, true, false);
        Thread.sleep(100);
        Assert.assertEquals("PreallocatedlogId", expectedPreAllocatedLogIDDuringInitialization + 2,
                entryLoggerAllocator.getPreallocatedLogId());
        Assert.assertNotEquals("EntryLog of this slot should not be null", null,
                entryLogger.entryLogManager.getCurrentLogOfSlot(0));
        Assert.assertNotEquals("Preallocation Future of this slot should not be null", null,
                entryLogger.entryLoggerAllocator.preallocation[0]);
        ledgerDirsOfSlot0.add(entryLogger.entryLogManager.getLedgerDirOfSlot(0));

        /*
         * creating newlog with allLedgerDirs full
         */
        entryLogger.createNewLog(0, true, true);
        Thread.sleep(100);
        Assert.assertEquals("PreallocatedlogId", expectedPreAllocatedLogIDDuringInitialization + 3,
                entryLoggerAllocator.getPreallocatedLogId());
        Assert.assertNotEquals("EntryLog of this slot should not be null", null,
                entryLogger.entryLogManager.getCurrentLogOfSlot(0));
        Assert.assertNotEquals("Preallocation Future of this slot should not be null", null,
                entryLogger.entryLoggerAllocator.preallocation[0]);
        ledgerDirsOfSlot0.add(entryLogger.entryLogManager.getLedgerDirOfSlot(0));

        /*
         * since it is single slot, ledgerDirs of slot0 would change
         */
        Assert.assertTrue("Ledgerdirs ", ledgerDirsOfSlot0.size() > 1);
    }

    @Test(timeout = 60000)
    public void testEntryLogCreationWithSingleSlotAndNoPreAllocation() throws Exception {
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        // set numberOfActiveEntryLogsPerLedgerDir to 0 to have only one slot
        int numberOfActiveEntryLogsPerLedgerDir = 0;

        // Creating a new configuration with a number of ledger directories.
        conf.setLedgerDirNames(ledgerDirs);
        conf.setIsForceGCAllowWhenNoSpace(true);
        // pre-allocation is not enabled
        conf.setEntryLogFilePreAllocationEnabled(false);
        conf.setNumberOfActiveEntryLogsPerLedgerDir(numberOfActiveEntryLogsPerLedgerDir);
        LedgerDirsManager ledgerDirsManager = new LedgerDirsManager(conf, conf.getLedgerDirs(),
                new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold()));
        EntryLogger entryLogger = new EntryLogger(conf, ledgerDirsManager);
        EntryLoggerAllocator entryLoggerAllocator = entryLogger.entryLoggerAllocator;
        Thread.sleep(200);
        int expectedPreAllocatedLogIDDuringInitialization = 0;
        Assert.assertEquals("PreallocatedlogId after initialization of Entrylogger",
                expectedPreAllocatedLogIDDuringInitialization, entryLoggerAllocator.getPreallocatedLogId());
        Assert.assertEquals("Preallocation Future of this slot should be null", null,
                entryLogger.entryLoggerAllocator.preallocation[0]);

        Assert.assertEquals("Next slot should always be 0 since numberOfActiveEntryLogsPerLedgerDir is set to false", 0,
                entryLogger.entryLogManager.getNextSlot());
        Assert.assertEquals("Next slot should always be 0 since numberOfActiveEntryLogsPerLedgerDir is set to false", 0,
                entryLogger.entryLogManager.getNextSlot());
        Set<File> ledgerDirsOfSlot0 = new HashSet<File>();
        ledgerDirsOfSlot0.add(entryLogger.entryLogManager.getLedgerDirOfSlot(0));

        entryLogger.createNewLog(0, false, false);
        Thread.sleep(100);
        Assert.assertEquals("PreallocatedlogId after initialization of Entrylogger",
                expectedPreAllocatedLogIDDuringInitialization + 1, entryLoggerAllocator.getPreallocatedLogId());
        ledgerDirsOfSlot0.add(entryLogger.entryLogManager.getLedgerDirOfSlot(0));

        entryLogger.createNewLog(0, true, false);
        Thread.sleep(100);
        Assert.assertEquals("PreallocatedlogId", expectedPreAllocatedLogIDDuringInitialization + 2,
                entryLoggerAllocator.getPreallocatedLogId());
        Assert.assertNotEquals("EntryLog of this slot should not be null", null,
                entryLogger.entryLogManager.getCurrentLogOfSlot(0));
        Assert.assertEquals("Preallocation Future of this slot should be null", null,
                entryLogger.entryLoggerAllocator.preallocation[0]);
        ledgerDirsOfSlot0.add(entryLogger.entryLogManager.getLedgerDirOfSlot(0));

        entryLogger.createNewLog(0, true, true);
        Thread.sleep(100);
        Assert.assertEquals("PreallocatedlogId", expectedPreAllocatedLogIDDuringInitialization + 3,
                entryLoggerAllocator.getPreallocatedLogId());
        Assert.assertNotEquals("EntryLog of this slot should not be null", null,
                entryLogger.entryLogManager.getCurrentLogOfSlot(0));
        Assert.assertEquals("Preallocation Future of this slot should be null", null,
                entryLogger.entryLoggerAllocator.preallocation[0]);
        ledgerDirsOfSlot0.add(entryLogger.entryLogManager.getLedgerDirOfSlot(0));

        Assert.assertTrue("Ledgerdirs ", ledgerDirsOfSlot0.size() > 1);
    }
}
