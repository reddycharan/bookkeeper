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
package org.apache.bookkeeper.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

import org.apache.bookkeeper.client.AsyncCallback.AddCallback;
import org.apache.bookkeeper.client.BKException.BKLedgerClosedException;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.meta.LongHierarchicalLedgerManagerFactory;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.test.MultiLedgerManagerMultiDigestTestCase;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Testing ledger write entry cases
 */
public class BookieWriteLedgerTest extends
        MultiLedgerManagerMultiDigestTestCase implements AddCallback {

    private final static Logger LOG = LoggerFactory
            .getLogger(BookieWriteLedgerTest.class);

    byte[] ledgerPassword = "aaa".getBytes();
    LedgerHandle lh, lh2;
    Enumeration<LedgerEntry> ls;

    // test related variables
    int numEntriesToWrite = 100;
    int maxInt = Integer.MAX_VALUE;
    Random rng; // Random Number Generator
    ArrayList<byte[]> entries1; // generated entries
    ArrayList<byte[]> entries2; // generated entries

    DigestType digestType;

    private static class SyncObj {
        volatile int counter;
        volatile int rc;

        public SyncObj() {
            counter = 0;
        }
    }

    @Override
    @Before
    public void setUp() throws Exception {
    	// 512K to speed up testLedgerCreateAdvWithLedgerIdInLoop2
    	baseConf.setSkipListSizeLimit(512 * 1024);
    	
        super.setUp();
        rng = new Random(System.currentTimeMillis()); // Initialize the Random
        // Number Generator
        entries1 = new ArrayList<byte[]>(); // initialize the entries list
        entries2 = new ArrayList<byte[]>(); // initialize the entries list
    }

    public BookieWriteLedgerTest(String ledgerManagerFactory,
            DigestType digestType) {
        super(5);
        this.digestType = digestType;
        // set ledger manager
        baseConf.setLedgerManagerFactoryClassName(ledgerManagerFactory);
        baseClientConf.setLedgerManagerFactoryClassName(ledgerManagerFactory);
    }

    /**
     * Verify write when few bookie failures in last ensemble and forcing
     * ensemble reformation
     */
    @Test(timeout=60000)
    public void testWithMultipleBookieFailuresInLastEnsemble() throws Exception {
        // Create a ledger
        lh = bkc.createLedger(5, 4, digestType, ledgerPassword);
        LOG.info("Ledger ID: " + lh.getId());
        for (int i = 0; i < numEntriesToWrite; i++) {
            ByteBuffer entry = ByteBuffer.allocate(4);
            entry.putInt(rng.nextInt(maxInt));
            entry.position(0);

            entries1.add(entry.array());
            lh.addEntry(entry.array());
        }
        // Start three more bookies
        startNewBookie();
        startNewBookie();
        startNewBookie();

        // Shutdown three bookies in the last ensemble and continue writing
        ArrayList<BookieSocketAddress> ensemble = lh.getLedgerMetadata()
                .getEnsembles().entrySet().iterator().next().getValue();
        killBookie(ensemble.get(0));
        killBookie(ensemble.get(1));
        killBookie(ensemble.get(2));

        int i = numEntriesToWrite;
        numEntriesToWrite = numEntriesToWrite + 50;
        for (; i < numEntriesToWrite; i++) {
            ByteBuffer entry = ByteBuffer.allocate(4);
            entry.putInt(rng.nextInt(maxInt));
            entry.position(0);

            entries1.add(entry.array());
            lh.addEntry(entry.array());
        }

        readEntries(lh, entries1);
        lh.close();
    }

    /**
     * Verify the functionality of Advanced Ledger which returns
     * LedgerHandleAdv. LedgerHandleAdv takes entryId for addEntry, and let
     * user manage entryId allocation.
     *
     * @throws Exception
     */
    @Test(timeout = 60000)
    public void testLedgerCreateAdv() throws Exception {
        // Create a ledger
        lh = bkc.createLedgerAdv(5, 3, 2, digestType, ledgerPassword);
        for (int i = 0; i < numEntriesToWrite; i++) {
            ByteBuffer entry = ByteBuffer.allocate(4);
            entry.putInt(rng.nextInt(maxInt));
            entry.position(0);

            entries1.add(entry.array());
            lh.addEntry(i, entry.array());
        }
        // Start one more bookies
        startNewBookie();

        // Shutdown one bookie in the last ensemble and continue writing
        ArrayList<BookieSocketAddress> ensemble = lh.getLedgerMetadata().getEnsembles().entrySet().iterator().next()
                .getValue();
        killBookie(ensemble.get(0));

        int i = numEntriesToWrite;
        numEntriesToWrite = numEntriesToWrite + 50;
        for (; i < numEntriesToWrite; i++) {
            ByteBuffer entry = ByteBuffer.allocate(4);
            entry.putInt(rng.nextInt(maxInt));
            entry.position(0);

            entries1.add(entry.array());
            lh.addEntry(i, entry.array());
        }

        readEntries(lh, entries1);
        lh.close();
    }

    /**
     * Verify the functionality of Advanced Ledger which accepts ledgerId as input and returns
     * LedgerHandleAdv. LedgerHandleAdv takes entryId for addEntry, and let
     * user manage entryId allocation.
     *
     * @throws Exception
     */
    @Test(timeout = 60000)
    public void testLedgerCreateAdvWithLedgerId() throws Exception {
        // Create a ledger
        long ledgerId = 0xABCDEF;
        lh = bkc.createLedgerAdv(ledgerId, 5, 3, 2, digestType, ledgerPassword);
        for (int i = 0; i < numEntriesToWrite; i++) {
            ByteBuffer entry = ByteBuffer.allocate(4);
            entry.putInt(rng.nextInt(maxInt));
            entry.position(0);

            entries1.add(entry.array());
            lh.addEntry(i, entry.array());
        }
        // Start one more bookies
        startNewBookie();

        // Shutdown one bookie in the last ensemble and continue writing
        ArrayList<BookieSocketAddress> ensemble = lh.getLedgerMetadata().getEnsembles().entrySet().iterator().next()
                .getValue();
        killBookie(ensemble.get(0));

        int i = numEntriesToWrite;
        numEntriesToWrite = numEntriesToWrite + 50;
        for (; i < numEntriesToWrite; i++) {
            ByteBuffer entry = ByteBuffer.allocate(4);
            entry.putInt(rng.nextInt(maxInt));
            entry.position(0);

            entries1.add(entry.array());
            lh.addEntry(i, entry.array());
        }

        readEntries(lh, entries1);
        lh.close();
        bkc.deleteLedger(ledgerId);
    }

    /**
     * Verify the functionality of Advanced Ledger which accepts ledgerId as
     * input and returns LedgerHandleAdv. LedgerHandleAdv takes entryId for
     * addEntry, and let user manage entryId allocation.
     * 
     * This testcase is mainly added for covering missing code coverage branches
     * in LedgerHandleAdv
     *
     * @throws Exception
     */
    @Test(timeout = 60000)
    public void testLedgerHandleAdvFunctionality() throws Exception {
        // Create a ledger
        long ledgerId = 0xABCDEF;
        lh = bkc.createLedgerAdv(ledgerId, 5, 3, 2, digestType, ledgerPassword);
        numEntriesToWrite = 3;

        ByteBuffer entry = ByteBuffer.allocate(4);
        entry.putInt(rng.nextInt(maxInt));
        entry.position(0);
        entries1.add(entry.array());
        lh.addEntry(0, entry.array());

        // here asyncAddEntry(final long entryId, final byte[] data, final
        // AddCallback cb, final Object ctx) method is
        // called which is not covered in any other testcase
        entry = ByteBuffer.allocate(4);
        entry.putInt(rng.nextInt(maxInt));
        entry.position(0);
        entries1.add(entry.array());
        CountDownLatch latch = new CountDownLatch(1);
        final int[] returnedRC = new int[1];
        lh.asyncAddEntry(1, entry.array(), new AddCallback() {
            @Override
            public void addComplete(int rc, LedgerHandle lh, long entryId, Object ctx) {
                CountDownLatch latch = (CountDownLatch) ctx;
                returnedRC[0] = rc;
                latch.countDown();
            }
        }, latch);
        latch.await();
        assertTrue("Returned code is expected to be OK", returnedRC[0] == BKException.Code.OK);

        // here addEntry is called with incorrect offset and length
        entry = ByteBuffer.allocate(4);
        entry.putInt(rng.nextInt(maxInt));
        entry.position(0);
        try {
            lh.addEntry(2, entry.array(), -3, 9);
            fail("AddEntry is called with negative offset and incorrect length, so it is expected to throw ArrayIndexOutOfBoundsException");
        } catch (ArrayIndexOutOfBoundsException exception) {
        }

        // here addEntry is called with corrected offset and length and it is
        // supposed to succeed
        entry = ByteBuffer.allocate(4);
        entry.putInt(rng.nextInt(maxInt));
        entry.position(0);
        entries1.add(entry.array());
        lh.addEntry(2, entry.array());

        // LedgerHandle is closed for write
        lh.close();

        // here addEntry is called even after the close of the LedgerHandle, so
        // it is expected to throw exception
        entry = ByteBuffer.allocate(4);
        entry.putInt(rng.nextInt(maxInt));
        entry.position(0);
        entries1.add(entry.array());
        try {
            lh.addEntry(3, entry.array());
            fail("AddEntry is called after the close of LedgerHandle, so it is expected to throw BKLedgerClosedException");
        } catch (BKLedgerClosedException exception) {
        }

        readEntries(lh, entries1);
        bkc.deleteLedger(ledgerId);
    }
    
    /**
     * In a loop create/write/delete the ledger with same ledgerId through
     * the functionality of Advanced Ledger which accepts ledgerId as input.
     *
     * @throws Exception
     */
    @Test(timeout = 60000)
    public void testLedgerCreateAdvWithLedgerIdInLoop() throws Exception {
        long ledgerId;
        int ledgerCount = 10;

        List<List<byte[]>> entryList = new ArrayList<List<byte[]>>();
        LedgerHandle[] lhArray = new LedgerHandle[ledgerCount];

        List<byte[]> tmpEntry;
        for (int lc = 0; lc < ledgerCount; lc++) {
            tmpEntry = new ArrayList<byte[]>();

            ledgerId = rng.nextLong();
            ledgerId &= Long.MAX_VALUE;
            if (!baseConf.getLedgerManagerFactoryClass().equals(LongHierarchicalLedgerManagerFactory.class)) {
                // since LongHierarchicalLedgerManager supports ledgerIds of decimal length upto 19 digits but other
                // LedgerManagers only upto 10 decimals
                ledgerId %= 9999999999L;
            }

            LOG.info("Iteration: {}  LedgerId: {}", lc, ledgerId);
            lh = bkc.createLedgerAdv(ledgerId, 5, 3, 2, digestType, ledgerPassword);
            lhArray[lc] = lh;

            for (int i = 0; i < numEntriesToWrite; i++) {
                ByteBuffer entry = ByteBuffer.allocate(4);
                entry.putInt(rng.nextInt(maxInt));
                entry.position(0);
                tmpEntry.add(entry.array());
                lh.addEntry(i, entry.array());
            }
            entryList.add(tmpEntry);
        }
        for (int lc = 0; lc < ledgerCount; lc++) {
            // Read and verify
            long lid = lhArray[lc].getId();
            LOG.info("readEntries for lc: {} ledgerId: {} ", lc, lhArray[lc].getId());
            readEntries(lhArray[lc], entryList.get(lc));
            lhArray[lc].close();
            bkc.deleteLedger(lid);
        }
    }

    /**
     * In a loop create/write/read/delete the ledger with ledgerId through the
     * functionality of Advanced Ledger which accepts ledgerId as input.
     * 
     * In this testcase (other testcases don't cover these conditions, hence new
     * testcase is added), we create entries which are greater than
     * SKIP_LIST_MAX_ALLOC_ENTRY size and tried to addEntries so that the total
     * length of data written in this testcase is much greater than
     * SKIP_LIST_SIZE_LIMIT, so that entries will be flushed from EntryMemTable
     * to persistent storage
     * 
     *
     * @throws Exception
     */
    @Test(timeout = 60000)
    public void testLedgerCreateAdvWithLedgerIdInLoop2() throws Exception {

        assertTrue("Here we are expecting Bookies are configured to use SortedLedgerStorage",
                baseConf.getSortedLedgerStorageEnabled());

        long ledgerId;
        int ledgerCount = 10;

        List<List<byte[]>> entryList = new ArrayList<List<byte[]>>();
        LedgerHandle[] lhArray = new LedgerHandle[ledgerCount];
        long skipListSizeLimit = baseConf.getSkipListSizeLimit();
        int skipListArenaMaxAllocSize = baseConf.getSkipListArenaMaxAllocSize();

        List<byte[]> tmpEntry;
        for (int lc = 0; lc < ledgerCount; lc++) {
            tmpEntry = new ArrayList<byte[]>();

            ledgerId = rng.nextLong();
            ledgerId &= Long.MAX_VALUE;
            if (!baseConf.getLedgerManagerFactoryClass().equals(LongHierarchicalLedgerManagerFactory.class)) {
                // since LongHierarchicalLedgerManager supports ledgerIds of
                // decimal length upto 19 digits but other
                // LedgerManagers only upto 10 decimals
                ledgerId %= 9999999999L;
            }

            LOG.debug("Iteration: {}  LedgerId: {}", lc, ledgerId);
            lh = bkc.createLedgerAdv(ledgerId, 5, 3, 2, digestType, ledgerPassword);
            lhArray[lc] = lh;

            long ledgerLength = 0;
            int i = 0;
            while (ledgerLength < ((4 * skipListSizeLimit) / ledgerCount)) {
                int length;
                if (rng.nextBoolean()) {
                    length = Math.abs(rng.nextInt()) % (skipListArenaMaxAllocSize);
                } else {
                    // here we want length to be random no. in the range of skipListArenaMaxAllocSize and
                    // 4*skipListArenaMaxAllocSize
                    length = (Math.abs(rng.nextInt()) % (skipListArenaMaxAllocSize * 3)) + skipListArenaMaxAllocSize;
                }
                byte[] data = new byte[length];
                rng.nextBytes(data);
                tmpEntry.add(data);
                lh.addEntry(i, data);
                ledgerLength += length;
                i++;
            }
            entryList.add(tmpEntry);
        }
        for (int lc = 0; lc < ledgerCount; lc++) {
            // Read and verify
            long lid = lhArray[lc].getId();
            LOG.debug("readEntries for lc: {} ledgerId: {} ", lc, lhArray[lc].getId());
            readEntriesAndValidateDataArray(lhArray[lc], entryList.get(lc));
            lhArray[lc].close();
            bkc.deleteLedger(lid);
        }
    }
    
    /**
     * Verify asynchronous writing when few bookie failures in last ensemble.
     */
    @Test(timeout=60000)
    public void testAsyncWritesWithMultipleFailuresInLastEnsemble()
            throws Exception {
        // Create ledgers
        lh = bkc.createLedger(5, 4, digestType, ledgerPassword);
        lh2 = bkc.createLedger(5, 4, digestType, ledgerPassword);

        LOG.info("Ledger ID-1: " + lh.getId());
        LOG.info("Ledger ID-2: " + lh2.getId());
        for (int i = 0; i < numEntriesToWrite; i++) {
            ByteBuffer entry = ByteBuffer.allocate(4);
            entry.putInt(rng.nextInt(maxInt));
            entry.position(0);

            entries1.add(entry.array());
            entries2.add(entry.array());
            lh.addEntry(entry.array());
            lh2.addEntry(entry.array());
        }
        // Start three more bookies
        startNewBookie();
        startNewBookie();
        startNewBookie();

        // Shutdown three bookies in the last ensemble and continue writing
        ArrayList<BookieSocketAddress> ensemble = lh.getLedgerMetadata()
                .getEnsembles().entrySet().iterator().next().getValue();
        killBookie(ensemble.get(0));
        killBookie(ensemble.get(1));
        killBookie(ensemble.get(2));

        // adding one more entry to both the ledgers async after multiple bookie
        // failures. This will do asynchronously modifying the ledger metadata
        // simultaneously.
        numEntriesToWrite++;
        ByteBuffer entry = ByteBuffer.allocate(4);
        entry.putInt(rng.nextInt(maxInt));
        entry.position(0);
        entries1.add(entry.array());
        entries2.add(entry.array());

        SyncObj syncObj1 = new SyncObj();
        SyncObj syncObj2 = new SyncObj();
        lh.asyncAddEntry(entry.array(), this, syncObj1);
        lh2.asyncAddEntry(entry.array(), this, syncObj2);

        // wait for all entries to be acknowledged for the first ledger
        synchronized (syncObj1) {
            while (syncObj1.counter < 1) {
                LOG.debug("Entries counter = " + syncObj1.counter);
                syncObj1.wait();
            }
            assertEquals(BKException.Code.OK, syncObj1.rc);
        }
        // wait for all entries to be acknowledged for the second ledger
        synchronized (syncObj2) {
            while (syncObj2.counter < 1) {
                LOG.debug("Entries counter = " + syncObj2.counter);
                syncObj2.wait();
            }
            assertEquals(BKException.Code.OK, syncObj2.rc);
        }

        // reading ledger till the last entry
        readEntries(lh, entries1);
        readEntries(lh2, entries2);
        lh.close();
        lh2.close();
    }

    /**
     * Verify Advanced asynchronous writing with entryIds in reverse order
     */
    @Test(timeout = 60000)
    public void testLedgerCreateAdvWithAsyncWritesWithBookieFailures() throws Exception {
        // Create ledgers
        lh = bkc.createLedgerAdv(5, 3, 2, digestType, ledgerPassword);
        lh2 = bkc.createLedgerAdv(5, 3, 2, digestType, ledgerPassword);

        LOG.info("Ledger ID-1: " + lh.getId());
        LOG.info("Ledger ID-2: " + lh2.getId());
        SyncObj syncObj1 = new SyncObj();
        SyncObj syncObj2 = new SyncObj();
        for (int i = numEntriesToWrite - 1; i >= 0; i--) {
            ByteBuffer entry = ByteBuffer.allocate(4);
            entry.putInt(rng.nextInt(maxInt));
            entry.position(0);
            try {
                entries1.add(0, entry.array());
                entries2.add(0, entry.array());
            } catch (Exception e) {
                e.printStackTrace();
            }
            lh.asyncAddEntry(i, entry.array(), 0, entry.capacity(), this, syncObj1);
            lh2.asyncAddEntry(i, entry.array(), 0, entry.capacity(), this, syncObj2);
        }
        // Start One more bookie and shutdown one from last ensemble before reading
        startNewBookie();
        ArrayList<BookieSocketAddress> ensemble = lh.getLedgerMetadata().getEnsembles().entrySet().iterator().next()
                .getValue();
        killBookie(ensemble.get(0));

        // Wait for all entries to be acknowledged for the first ledger
        synchronized (syncObj1) {
            while (syncObj1.counter < numEntriesToWrite) {
                syncObj1.wait();
            }
            assertEquals(BKException.Code.OK, syncObj1.rc);
        }
        // Wait for all entries to be acknowledged for the second ledger
        synchronized (syncObj2) {
            while (syncObj2.counter < numEntriesToWrite) {
                syncObj2.wait();
            }
            assertEquals(BKException.Code.OK, syncObj2.rc);
        }

        // Reading ledger till the last entry
        readEntries(lh, entries1);
        readEntries(lh2, entries2);
        lh.close();
        lh2.close();
    }

    /**
     * Verify Advanced asynchronous writing with entryIds in pseudo random order with bookie failures between writes
     */
    @Test(timeout = 60000)
    public void testLedgerCreateAdvWithRandomAsyncWritesWithBookieFailuresBetweenWrites() throws Exception {
        // Create ledgers
        lh = bkc.createLedgerAdv(5, 3, 2, digestType, ledgerPassword);
        lh2 = bkc.createLedgerAdv(5, 3, 2, digestType, ledgerPassword);

        LOG.info("Ledger ID-1: " + lh.getId());
        LOG.info("Ledger ID-2: " + lh2.getId());
        SyncObj syncObj1 = new SyncObj();
        SyncObj syncObj2 = new SyncObj();
        int batchSize = 5;
        int i, j;

        // Fill the result buffers first
        for (i = 0; i < numEntriesToWrite; i++) {
            ByteBuffer entry = ByteBuffer.allocate(4);

            entry.putInt(rng.nextInt(maxInt));
            entry.position(0);
            try {
                entries1.add(0, entry.array());
                entries2.add(0, entry.array());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        for (i = 0; i < batchSize; i++) {
            for (j = i; j < numEntriesToWrite; j = j + batchSize) {
                byte[] entry1 = entries1.get(j);
                byte[] entry2 = entries2.get(j);
                lh.asyncAddEntry(j, entry1, 0, entry1.length, this, syncObj1);
                lh2.asyncAddEntry(j, entry2, 0, entry2.length, this, syncObj2);
                if (j == numEntriesToWrite/2) {
                    // Start One more bookie and shutdown one from last ensemble at half-way
                    startNewBookie();
                    ArrayList<BookieSocketAddress> ensemble = lh.getLedgerMetadata().getEnsembles().entrySet()
                            .iterator().next().getValue();
                    killBookie(ensemble.get(0));
                }
            }
        }

        // Wait for all entries to be acknowledged for the first ledger
        synchronized (syncObj1) {
            while (syncObj1.counter < numEntriesToWrite) {
                syncObj1.wait();
            }
            assertEquals(BKException.Code.OK, syncObj1.rc);
        }
        // Wait for all entries to be acknowledged for the second ledger
        synchronized (syncObj2) {
            while (syncObj2.counter < numEntriesToWrite) {
                syncObj2.wait();
            }
            assertEquals(BKException.Code.OK, syncObj2.rc);
        }

        // Reading ledger till the last entry
        readEntries(lh, entries1);
        readEntries(lh2, entries2);
        lh.close();
        lh2.close();
    }

    /**
     * Verify Advanced asynchronous writing with entryIds in pseudo random order
     */
    @Test(timeout = 60000)
    public void testLedgerCreateAdvWithRandomAsyncWritesWithBookieFailures() throws Exception {
        // Create ledgers
        lh = bkc.createLedgerAdv(5, 3, 2, digestType, ledgerPassword);
        lh2 = bkc.createLedgerAdv(5, 3, 2, digestType, ledgerPassword);

        LOG.info("Ledger ID-1: " + lh.getId());
        LOG.info("Ledger ID-2: " + lh2.getId());
        SyncObj syncObj1 = new SyncObj();
        SyncObj syncObj2 = new SyncObj();
        int batchSize = 5;
        int i, j;

        // Fill the result buffers first
        for (i = 0; i < numEntriesToWrite; i++) {
            ByteBuffer entry = ByteBuffer.allocate(4);

            entry.putInt(rng.nextInt(maxInt));
            entry.position(0);
            try {
                entries1.add(0, entry.array());
                entries2.add(0, entry.array());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        for (i = 0; i < batchSize; i++) {
            for (j = i; j < numEntriesToWrite; j = j + batchSize) {
                byte[] entry1 = entries1.get(j);
                byte[] entry2 = entries2.get(j);
                lh.asyncAddEntry(j, entry1, 0, entry1.length, this, syncObj1);
                lh2.asyncAddEntry(j, entry2, 0, entry2.length, this, syncObj2);
            }
        }
        // Start One more bookie and shutdown one from last ensemble before reading
        startNewBookie();
        ArrayList<BookieSocketAddress> ensemble = lh.getLedgerMetadata().getEnsembles().entrySet().iterator().next()
                .getValue();
        killBookie(ensemble.get(0));

        // Wait for all entries to be acknowledged for the first ledger
        synchronized (syncObj1) {
            while (syncObj1.counter < numEntriesToWrite) {
                syncObj1.wait();
            }
            assertEquals(BKException.Code.OK, syncObj1.rc);
        }
        // Wait for all entries to be acknowledged for the second ledger
        synchronized (syncObj2) {
            while (syncObj2.counter < numEntriesToWrite) {
                syncObj2.wait();
            }
            assertEquals(BKException.Code.OK, syncObj2.rc);
        }

        // Reading ledger till the last entry
        readEntries(lh, entries1);
        readEntries(lh2, entries2);
        lh.close();
        lh2.close();
    }

    /**
     * Skips few entries before closing the ledger and assert that the
     * lastAddConfirmed is right before our skipEntryId.
     *
     * @throws Exception
     */
    @Test(timeout = 60000)
    public void testLedgerCreateAdvWithSkipEntries() throws Exception {
        long ledgerId;
        SyncObj syncObj1 = new SyncObj();

        // Create a ledger
        lh = bkc.createLedgerAdv(5, 3, 2, digestType, ledgerPassword);
        // Save ledgerId to reopen the ledger
        ledgerId = lh.getId();
        LOG.info("Ledger ID: " + ledgerId);
        int skipEntryId = rng.nextInt(numEntriesToWrite - 1);
        for (int i = numEntriesToWrite - 1; i >= 0; i--) {
            ByteBuffer entry = ByteBuffer.allocate(4);
            entry.putInt(rng.nextInt(maxInt));
            entry.position(0);
            try {
                entries1.add(0, entry.array());
            } catch (Exception e) {
                e.printStackTrace();
            }
            if (i == skipEntryId) {
                LOG.info("Skipping entry:{}", skipEntryId);
                continue;
            }
            lh.asyncAddEntry(i, entry.array(), 0, entry.capacity(), this, syncObj1);
        }
        // wait for all entries to be acknowledged for the first ledger
        synchronized (syncObj1) {
            while (syncObj1.counter < skipEntryId) {
                syncObj1.wait();
            }
            assertEquals(BKException.Code.OK, syncObj1.rc);
        }
        // Close the ledger
        lh.close();
        // Open the ledger
        lh = bkc.openLedger(ledgerId, digestType, ledgerPassword);
        assertEquals(lh.lastAddConfirmed, skipEntryId - 1);
        lh.close();
    }

    /**
     * Verify the functionality LedgerHandleAdv addEntry with duplicate entryIds
     *
     * @throws Exception
     */
    @Test(timeout = 60000)
    public void testLedgerCreateAdvSyncAddDuplicateEntryIds() throws Exception {
        // Create a ledger
        lh = bkc.createLedgerAdv(5, 3, 2, digestType, ledgerPassword);
        LOG.info("Ledger ID: " + lh.getId());
        for (int i = 0; i < numEntriesToWrite; i++) {
            ByteBuffer entry = ByteBuffer.allocate(4);
            entry.putInt(rng.nextInt(maxInt));
            entry.position(0);

            entries1.add(entry.array());
            lh.addEntry(i, entry.array());
            entry.position(0);
        }
        readEntries(lh, entries1);

        int dupEntryId = rng.nextInt(numEntriesToWrite - 1);

        try {
            ByteBuffer entry = ByteBuffer.allocate(4);
            entry.putInt(rng.nextInt(maxInt));
            entry.position(0);
            lh.addEntry(dupEntryId, entry.array());
            fail("Expected exception not thrown");
        } catch (BKException e) {
            // This test expects DuplicateEntryIdException
            assertEquals(e.getCode(), BKException.Code.DuplicateEntryIdException);
        }
        lh.close();
    }

    /**
     * Verify the functionality LedgerHandleAdv asyncAddEntry with duplicate
     * entryIds
     *
     * @throws Exception
     */
    @Test(timeout = 60000)
    public void testLedgerCreateAdvSyncAsyncAddDuplicateEntryIds() throws Exception {
        long ledgerId;
        SyncObj syncObj1 = new SyncObj();
        SyncObj syncObj2 = new SyncObj();

        // Create a ledger
        lh = bkc.createLedgerAdv(5, 3, 2, digestType, ledgerPassword);
        // Save ledgerId to reopen the ledger
        ledgerId = lh.getId();
        LOG.info("Ledger ID: " + ledgerId);
        for (int i = numEntriesToWrite - 1; i >= 0; i--) {
            ByteBuffer entry = ByteBuffer.allocate(4);
            entry.putInt(rng.nextInt(maxInt));
            entry.position(0);
            try {
                entries1.add(0, entry.array());
            } catch (Exception e) {
                e.printStackTrace();
            }
            lh.asyncAddEntry(i, entry.array(), 0, entry.capacity(), this, syncObj1);
            if (rng.nextBoolean()) {
                // Attempt to write the same entry
                lh.asyncAddEntry(i, entry.array(), 0, entry.capacity(), this, syncObj2);
                synchronized (syncObj2) {
                    while (syncObj2.counter < 1) {
                        syncObj2.wait();
                    }
                    assertEquals(BKException.Code.DuplicateEntryIdException, syncObj2.rc);
                }
            }
        }
        // Wait for all entries to be acknowledged for the first ledger
        synchronized (syncObj1) {
            while (syncObj1.counter < numEntriesToWrite) {
                syncObj1.wait();
            }
            assertEquals(BKException.Code.OK, syncObj1.rc);
        }
        // Close the ledger
        lh.close();
    }

    private void readEntries(LedgerHandle lh, List<byte[]> entries) throws InterruptedException, BKException {
        ls = lh.readEntries(0, numEntriesToWrite - 1);
        int index = 0;
        while (ls.hasMoreElements()) {
            ByteBuffer origbb = ByteBuffer.wrap(entries.get(index++));
            Integer origEntry = origbb.getInt();
            ByteBuffer result = ByteBuffer.wrap(ls.nextElement().getEntry());
            LOG.debug("Length of result: " + result.capacity());
            LOG.debug("Original entry: " + origEntry);
            Integer retrEntry = result.getInt();
            LOG.debug("Retrieved entry: " + retrEntry);
            assertTrue("Checking entry " + index + " for equality", origEntry
                    .equals(retrEntry));
        }
    }

    private void readEntriesAndValidateDataArray(LedgerHandle lh, List<byte[]> entries)
            throws InterruptedException, BKException {
        ls = lh.readEntries(0, entries.size() - 1);
        int index = 0;
        while (ls.hasMoreElements()) {
            byte[] originalData = entries.get(index++);
            byte[] receivedData = ls.nextElement().getEntry();
            LOG.debug("Length of originalData: " + originalData.length);
            LOG.debug("Length of receivedData: " + receivedData.length);
            assertTrue(
                    String.format("LedgerID: %d EntryID: %d OriginalDataLength: %d ReceivedDataLength: %d", lh.getId(),
                            (index - 1), originalData.length, receivedData.length),
                    originalData.length == receivedData.length);
            assertTrue(String.format("Checking LedgerID: %d EntryID: %d  for equality", lh.getId(), (index - 1)),
                    Arrays.equals(originalData, receivedData));
        }
    }
    
    @Override
    public void addComplete(int rc, LedgerHandle lh, long entryId, Object ctx) {
        SyncObj x = (SyncObj) ctx;
        synchronized (x) {
            x.rc = rc;
            x.counter++;
            x.notify();
        }
    }
}
