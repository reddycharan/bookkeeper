package org.apache.bookkeeper.client;

import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.function.Consumer;

import org.apache.bookkeeper.bookie.SortedLedgerStorage;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.test.BaseTestCase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BookKeeperClientTestsWithBookieErrors extends BaseTestCase {
    private final static Logger LOG = LoggerFactory.getLogger(BookKeeperTest.class);
    private final static int NUM_BOOKIES = 3;
    // The amount of sleeptime to sleep in injectSleepWhileRead fault injection
    private final long sleepTime;
    // Fault injection which would sleep for sleepTime before returning readEntry call
    private final Consumer<ByteBuffer> injectSleepWhileRead;
    // Fault injection which would corrupt the entry data before returning readEntry call
    private final Consumer<ByteBuffer> injectCorruptData;
    // The ordered list of injections for the Bookies (LedgerStorage). The first bookie to
    // get readEntry call will use the first faultInjection, and the second bookie to get readentry call
    // will use the second one and so on..
    // It is assumed that there would be faultInjection for each Bookie. So if there aren't NUM_BOOKIES num of
    // faulInjections in this list then it will fail with NullPointerException
    static private List<Consumer<ByteBuffer>> faultInjections = new ArrayList<Consumer<ByteBuffer>>();
    // This map is used for storing LedgerStorage and the corresponding faultInjection,
    // according to the faultInjections list
    static private HashMap<MockSortedLedgerStorage, Consumer<ByteBuffer>> storageFaultInjectionsMap = new HashMap<MockSortedLedgerStorage, Consumer<ByteBuffer>>();
    // Lock object for synchronizing injectCorruptData and faultInjections
    static final Object lock = new Object();

    private DigestType digestType;

    public BookKeeperClientTestsWithBookieErrors(DigestType digestType) {
        super(NUM_BOOKIES);
        this.digestType = digestType;
        baseConf.setLedgerStorageClass(MockSortedLedgerStorage.class.getName());

        // this fault injection will corrupt the entry data by modifying the last byte of the entry
        injectCorruptData = (byteBuf) -> {
            int lastByteIndex = byteBuf.limit() - 1;
            byteBuf.put(lastByteIndex, (byte) (byteBuf.get(lastByteIndex) - 1));
        };

        // this fault injection, will sleep for ReadEntryTimeout+2 secs before returning the readEntry call
        sleepTime = (baseClientConf.getReadEntryTimeout() + 2) * 1000;
        injectSleepWhileRead = (byteBuf) -> {
            try {
                Thread.sleep(sleepTime);
            } catch (InterruptedException e) {
            }
        };
    }

    @Before
    public void setUp() throws Exception {
        faultInjections.clear();
        storageFaultInjectionsMap.clear();
        super.setUp();
    }

    // Mock SortedLedgerStorage to simulate Fault Injection
    static class MockSortedLedgerStorage extends SortedLedgerStorage {
        public MockSortedLedgerStorage() {
            super();
        }

        @Override
        public ByteBuffer getEntry(long ledgerId, long entryId) throws IOException {
            Consumer<ByteBuffer> faultInjection;
            synchronized (lock) {
                faultInjection = storageFaultInjectionsMap.get(this);
                if (faultInjection == null) {
                    int readLedgerStorageIndex = storageFaultInjectionsMap.size();
                    faultInjection = faultInjections.get(readLedgerStorageIndex);
                    storageFaultInjectionsMap.put(this, faultInjection);
                }
            }
            ByteBuffer byteBuf = super.getEntry(ledgerId, entryId);
            faultInjection.accept(byteBuf);            
            return byteBuf;
        }
    }

    // In this testcase all the bookies will return corrupt entry
    @Test(timeout = 60000)
    public void testBookkeeperAllDigestErrors() throws Exception {
        ClientConfiguration conf = new ClientConfiguration().setZkServers(zkUtil.getZooKeeperConnectString());
        BookKeeper bkc = new BookKeeper(conf);

        DigestType digestCorrect = digestType;
        byte[] passwd = "AAAAAAA".getBytes();

        // all the bookies need to return corrupt data
        faultInjections.add(injectCorruptData);
        faultInjections.add(injectCorruptData);
        faultInjections.add(injectCorruptData);

        LedgerHandle wlh = bkc.createLedger(3, 3, 2, digestCorrect, passwd);
        long id = wlh.getId();
        for (int i = 0; i < 10; i++) {
            wlh.addEntry("foobarfoo".getBytes());
        }
        wlh.close();

        LedgerHandle rlh = bkc.openLedger(id, digestCorrect, passwd);
        try {
            rlh.readEntries(4, 4);
            fail("It is expected to fail with BKDigestMatchException");
        } catch (BKException.BKDigestMatchException e) {
        }
        rlh.close();
        bkc.close();
    }

    // In this testcase first two bookies will sleep (for ReadEntryTimeout+2 secs) before returning the data,
    // and the last one will return corrupt data
    @Test(timeout = 60000)
    public void testBKReadFirstTimeoutThenDigestError() throws Exception {
        ClientConfiguration conf = new ClientConfiguration().setZkServers(zkUtil.getZooKeeperConnectString());
        BookKeeper bkc = new BookKeeper(conf);

        DigestType digestCorrect = digestType;
        byte[] passwd = "AAAAAAA".getBytes();

        faultInjections.add(injectSleepWhileRead);
        faultInjections.add(injectSleepWhileRead);
        faultInjections.add(injectCorruptData);

        LedgerHandle wlh = bkc.createLedger(3, 3, 2, digestCorrect, passwd);
        long id = wlh.getId();
        for (int i = 0; i < 10; i++) {
            wlh.addEntry("foobarfoo".getBytes());
        }
        wlh.close();

        LedgerHandle rlh = bkc.openLedger(id, digestCorrect, passwd);
        try {
            rlh.readEntries(4, 4);
            fail("It is expected to fail with BKDigestMatchException");
        } catch (BKException.BKDigestMatchException e) {
        }
        rlh.close();
        bkc.close();
    }

    // In this testcase first one will return corrupt data and the last two bookies will
    // sleep (for ReadEntryTimeout+2 secs) before returning the data
    @Test(timeout = 60000)
    public void testBKReadFirstDigestErrorThenTimeout() throws Exception {
        ClientConfiguration conf = new ClientConfiguration().setZkServers(zkUtil.getZooKeeperConnectString());
        BookKeeper bkc = new BookKeeper(conf);

        DigestType digestCorrect = digestType;
        byte[] passwd = "AAAAAAA".getBytes();

        faultInjections.add(injectCorruptData);
        faultInjections.add(injectSleepWhileRead);
        faultInjections.add(injectSleepWhileRead);

        LedgerHandle wlh = bkc.createLedger(3, 3, 2, digestCorrect, passwd);
        long id = wlh.getId();
        for (int i = 0; i < 10; i++) {
            wlh.addEntry("foobarfoo".getBytes());
        }
        wlh.close();

        LedgerHandle rlh = bkc.openLedger(id, digestCorrect, passwd);
        try {
            rlh.readEntries(4, 4);
            fail("It is expected to fail with BKDigestMatchException");
        } catch (BKException.BKDigestMatchException e) {
        }
        rlh.close();
        bkc.close();
    }

    // In this testcase first two bookies are killed before making the readentry call
    // and the last one will return corrupt data
    @Test(timeout = 60000)
    public void testBKReadFirstBookiesDownThenDigestError() throws Exception {
        ClientConfiguration conf = new ClientConfiguration().setZkServers(zkUtil.getZooKeeperConnectString());
        BookKeeper bkc = new BookKeeper(conf);

        DigestType digestCorrect = digestType;
        byte[] passwd = "AAAAAAA".getBytes();

        faultInjections.add(injectCorruptData);

        LedgerHandle wlh = bkc.createLedger(3, 3, 2, digestCorrect, passwd);
        long id = wlh.getId();
        wlh.addEntry("foobarfoo".getBytes());
        wlh.close();

        super.killBookie(0);
        super.killBookie(1);

        Thread.sleep(500);

        LedgerHandle rlh = bkc.openLedger(id, digestCorrect, passwd);
        try {
            rlh.readEntries(0, 0);
            fail("It is expected to fail with BKDigestMatchException");
        } catch (BKException.BKDigestMatchException e) {
        }
        rlh.close();
        bkc.close();
    }

    // In this testcase all the bookies will sleep (for ReadEntryTimeout+2 secs) before returning the data
    @Test(timeout = 60000)
    public void testBKReadAllTimeouts() throws Exception {
        ClientConfiguration conf = new ClientConfiguration().setZkServers(zkUtil.getZooKeeperConnectString());
        BookKeeper bkc = new BookKeeper(conf);

        DigestType digestCorrect = digestType;
        byte[] passwd = "AAAAAAA".getBytes();

        faultInjections.add(injectSleepWhileRead);
        faultInjections.add(injectSleepWhileRead);
        faultInjections.add(injectSleepWhileRead);

        LedgerHandle wlh = bkc.createLedger(3, 3, 2, digestCorrect, passwd);
        long id = wlh.getId();
        for (int i = 0; i < 10; i++) {
            wlh.addEntry("foobarfoo".getBytes());
        }
        wlh.close();

        LedgerHandle rlh = bkc.openLedger(id, digestCorrect, passwd);
        try {
            rlh.readEntries(4, 4);
            fail("It is expected to fail with BKTimeoutException");
        } catch (BKException.BKTimeoutException e) {
        }
        rlh.close();
        bkc.close();
    }

    // In this testcase first two bookies will sleep (for ReadEntryTimeout+2 secs) before returning the data,
    // but the last one will return as expected
    @Test(timeout = 60000)
    public void testBKReadTwoBookiesTimeout() throws Exception {
        ClientConfiguration conf = new ClientConfiguration().setZkServers(zkUtil.getZooKeeperConnectString());
        BookKeeper bkc = new BookKeeper(conf);

        DigestType digestCorrect = digestType;
        byte[] passwd = "AAAAAAA".getBytes();

        faultInjections.add(injectSleepWhileRead);
        faultInjections.add(injectSleepWhileRead);
        faultInjections.add((byteBuf) -> {
        });

        LedgerHandle wlh = bkc.createLedger(3, 3, 2, digestCorrect, passwd);
        long id = wlh.getId();
        for (int i = 0; i < 10; i++) {
            wlh.addEntry("foobarfoo".getBytes());
        }
        wlh.close();

        LedgerHandle rlh = bkc.openLedger(id, digestCorrect, passwd);
        LedgerEntry entry = rlh.readEntries(4, 4).nextElement();
        Assert.assertTrue("The read Entry should match with what have been written",
                (new String(entry.getEntry())).equals("foobarfoo"));
        rlh.close();
        bkc.close();
    }

    // In this testcase first two bookies return the corrupt data,
    // but the last one will return as expected
    @Test(timeout = 60000)
    public void testBKReadTwoBookiesWithDigestError() throws Exception {
        ClientConfiguration conf = new ClientConfiguration().setZkServers(zkUtil.getZooKeeperConnectString());
        BookKeeper bkc = new BookKeeper(conf);

        DigestType digestCorrect = digestType;
        byte[] passwd = "AAAAAAA".getBytes();

        faultInjections.add(injectCorruptData);
        faultInjections.add(injectCorruptData);
        faultInjections.add((byteBuf) -> {
        });

        LedgerHandle wlh = bkc.createLedger(3, 3, 2, digestCorrect, passwd);
        long id = wlh.getId();
        for (int i = 0; i < 10; i++) {
            wlh.addEntry("foobarfoo".getBytes());
        }
        wlh.close();

        LedgerHandle rlh = bkc.openLedger(id, digestCorrect, passwd);
        LedgerEntry entry = rlh.readEntries(4, 4).nextElement();
        Assert.assertTrue("The read Entry should match with what have been written",
                (new String(entry.getEntry())).equals("foobarfoo"));
        rlh.close();
        bkc.close();
    }
}
