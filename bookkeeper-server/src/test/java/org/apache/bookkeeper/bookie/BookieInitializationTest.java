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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.BindException;
import java.net.InetAddress;
import java.net.URL;
import java.nio.ByteBuffer;
import java.security.AccessControlException;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.bookkeeper.bookie.BookieException.DirsPartitionDuplicationException;
import org.apache.bookkeeper.bookie.Journal.LastLogMark;
import org.apache.bookkeeper.bookie.LedgerDirsManager.NoWritableLedgerDirException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.replication.ReplicationException.CompatibilityException;
import org.apache.bookkeeper.replication.ReplicationException.UnavailableException;
import org.apache.bookkeeper.ssl.SecurityException;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stats.StatsProvider;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.test.PortManager;
import org.apache.bookkeeper.util.DiskChecker;
import org.apache.bookkeeper.util.ReflectionUtils;
import org.apache.bookkeeper.zookeeper.ZooKeeperClient;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Testing bookie initialization cases
 */
public class BookieInitializationTest extends BookKeeperClusterTestCase {
    private static final Logger LOG = LoggerFactory
            .getLogger(BookieInitializationTest.class);

    private static ObjectMapper om = new ObjectMapper();

    public BookieInitializationTest() {
        super(0);
    }

    private static class MockBookie extends Bookie {
        MockBookie(ServerConfiguration conf) throws IOException,
                KeeperException, InterruptedException, BookieException {
            super(conf);
        }

        void testRegisterBookie(ServerConfiguration conf) throws IOException {
            super.doRegisterBookie();
        }
    }

    /**
     * Verify the bookie server exit code. On ZooKeeper exception, should return
     * exit code ZK_REG_FAIL = 4
     */
    @Test(timeout = 20000)
    public void testExitCodeZK_REG_FAIL() throws Exception {
        File tmpDir = createTempDir("bookie", "test");

        final ServerConfiguration conf = TestBKConfiguration.newServerConfiguration()
                .setZkServers(null).setJournalDirName(tmpDir.getPath())
                .setLedgerDirNames(new String[] { tmpDir.getPath() });

        // simulating ZooKeeper exception by assigning a closed zk client to bk
        BookieServer bkServer = new BookieServer(conf) {
            protected Bookie newBookie(ServerConfiguration conf)
                    throws IOException, KeeperException, InterruptedException,
                    BookieException {
                MockBookie bookie = new MockBookie(conf);
                bookie.zk = zkc;
                zkc.close();
                return bookie;
            };
        };

        bkServer.start();
        bkServer.join();
        Assert.assertEquals("Failed to return ExitCode.ZK_REG_FAIL",
                ExitCode.ZK_REG_FAIL, bkServer.getExitCode());
    }

    @Test(timeout = 20000)
    public void testBookieRegistrationWithSameZooKeeperClient() throws Exception {
        File tmpDir = createTempDir("bookie", "test");

        final ServerConfiguration conf = TestBKConfiguration.newServerConfiguration()
                .setZkServers(null).setJournalDirName(tmpDir.getPath())
                .setLedgerDirNames(new String[] { tmpDir.getPath() });

        final String bkRegPath = conf.getZkAvailableBookiesPath() + "/"
                + InetAddress.getLocalHost().getHostAddress() + ":"
                + conf.getBookiePort();

        MockBookie b = new MockBookie(conf);
        b.zk = zkc;
        b.testRegisterBookie(conf);
        Assert.assertNotNull("Bookie registration node doesn't exists!",
                             zkc.exists(bkRegPath, false));

        // test register bookie again if the registeration node is created by itself.
        b.testRegisterBookie(conf);
        Assert.assertNotNull("Bookie registration node doesn't exists!",
                zkc.exists(bkRegPath, false));
    }

    /**
     * Verify the bookie reg. Restarting bookie server will wait for the session
     * timeout when previous reg node exists in zk. On zNode delete event,
     * should continue startup
     */
    @Test(timeout = 20000)
    public void testBookieRegistration() throws Exception {
        File tmpDir = createTempDir("bookie", "test");

        final ServerConfiguration conf = TestBKConfiguration.newServerConfiguration()
                .setZkServers(null).setJournalDirName(tmpDir.getPath())
                .setLedgerDirNames(new String[] { tmpDir.getPath() });

        final String bkRegPath = conf.getZkAvailableBookiesPath() + "/"
                + InetAddress.getLocalHost().getHostAddress() + ":"
                + conf.getBookiePort();

        MockBookie b = new MockBookie(conf);
        b.zk = zkc;
        b.testRegisterBookie(conf);
        Stat bkRegNode1 = zkc.exists(bkRegPath, false);
        Assert.assertNotNull("Bookie registration node doesn't exists!",
                bkRegNode1);

        // simulating bookie restart, on restart bookie will create new
        // zkclient and doing the registration.
        ZooKeeperClient newZk = createNewZKClient();
        b.zk = newZk;

        try {
            // deleting the znode, so that the bookie registration should
            // continue successfully on NodeDeleted event
            new Thread(() -> {
                try {
                    Thread.sleep(conf.getZkTimeout() / 3);
                    zkc.delete(bkRegPath, -1);
                } catch (Exception e) {
                    // Not handling, since the testRegisterBookie will fail
                    LOG.error("Failed to delete the znode :" + bkRegPath, e);
                }
            }).start();
            try {
                b.testRegisterBookie(conf);
            } catch (IOException e) {
                Throwable t = e.getCause();
                if (t instanceof KeeperException) {
                    KeeperException ke = (KeeperException) t;
                    Assert.assertTrue("ErrorCode:" + ke.code()
                            + ", Registration node exists",
                        ke.code() != KeeperException.Code.NODEEXISTS);
                }
                throw e;
            }

            // verify ephemeral owner of the bkReg znode
            Stat bkRegNode2 = newZk.exists(bkRegPath, false);
            Assert.assertNotNull("Bookie registration has been failed", bkRegNode2);
            Assert.assertTrue("Bookie is referring to old registration znode:"
                + bkRegNode1 + ", New ZNode:" + bkRegNode2, bkRegNode1
                .getEphemeralOwner() != bkRegNode2.getEphemeralOwner());
        } finally {
            newZk.close();
        }
    }

    /**
     * Verify the bookie registration, it should throw
     * KeeperException.NodeExistsException if the znode still exists even after
     * the zk session timeout.
     */
    @Test(timeout = 30000)
    public void testRegNodeExistsAfterSessionTimeOut() throws Exception {
        File tmpDir = createTempDir("bookie", "test");

        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration().setZkServers(null)
                .setJournalDirName(tmpDir.getPath()).setLedgerDirNames(
                        new String[] { tmpDir.getPath() });

        String bkRegPath = conf.getZkAvailableBookiesPath() + "/"
                + InetAddress.getLocalHost().getHostAddress() + ":"
                + conf.getBookiePort();

        MockBookie b = new MockBookie(conf);
        b.zk = zkc;
        b.testRegisterBookie(conf);
        Stat bkRegNode1 = zkc.exists(bkRegPath, false);
        Assert.assertNotNull("Bookie registration node doesn't exists!",
                bkRegNode1);

        // simulating bookie restart, on restart bookie will create new
        // zkclient and doing the registration.
        ZooKeeperClient newzk = createNewZKClient();
        b.zk = newzk;
        try {
            b.testRegisterBookie(conf);
            fail("Should throw NodeExistsException as the znode is not getting expired");
        } catch (IOException e) {
            Throwable t = e.getCause();
            if (t instanceof KeeperException) {
                KeeperException ke = (KeeperException) t;
                Assert.assertTrue("ErrorCode:" + ke.code()
                        + ", Registration node doesn't exists",
                        ke.code() == KeeperException.Code.NODEEXISTS);

                // verify ephemeral owner of the bkReg znode
                Stat bkRegNode2 = newzk.exists(bkRegPath, false);
                Assert.assertNotNull("Bookie registration has been failed",
                        bkRegNode2);
                Assert.assertTrue(
                        "Bookie wrongly registered. Old registration znode:"
                                + bkRegNode1 + ", New znode:" + bkRegNode2,
                        bkRegNode1.getEphemeralOwner() == bkRegNode2
                                .getEphemeralOwner());
                return;
            }
            throw e;
        } finally {
            newzk.close();
        }
    }

    /**
     * Verify user cannot start if user is in permittedStartupUsers conf list BKException BKUnauthorizedAccessException
     * if cannot start
     */
    @Test(timeout = 20000)
    public void testUserNotPermittedToStart() throws Exception {
        File tmpDir = createTempDir("bookie", "test");

        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        int port = 12555;
        conf.setZkServers(null).setBookiePort(port).setJournalDirName(tmpDir.getPath())
                .setLedgerDirNames(new String[] { tmpDir.getPath() });
        String userString = "jerrySeinfeld,elaineBennis,kramer,soupNazi,,newman,";
        conf.setPermittedStartupUsers(userString);
        BookieServer bs1 = null;
        try {
            bs1 = new BookieServer(conf);
            Assert.fail("Bookkeeper should not have started since current user isn't in permittedStartupUsers");
        } catch (AccessControlException buae) {
            // Expected
        } finally {
            if (bs1 != null && bs1.isRunning()) {
                bs1.shutdown();
            }
        }
    }

    /**
     * Verify user cannot start if user is in permittedStartupUsers conf list BKException BKUnauthorizedAccessException
     * if cannot start
     */
    @Test(timeout = 20000)
    public void testUserPermittedToStart() throws Exception {
        File tmpDir = createTempDir("bookie", "test");

        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        int port = 12555;
        conf.setZkServers(null).setBookiePort(port).setJournalDirName(tmpDir.getPath())
                .setLedgerDirNames(new String[] { tmpDir.getPath() });
        // Comma at front
        String userString = ",jerrySeinfeld, elaineBennis,kramer, " + System.getProperty("user.name");
        conf.setPermittedStartupUsers(userString);
        BookieServer bs1 = null;
        try {
            bs1 = new BookieServer(conf);
            bs1.start();
        } catch (AccessControlException buae) {
            Assert.fail("Bookkeeper should have started since current user is in permittedStartupUsers");
        } finally {
            if (bs1 != null && bs1.isRunning()) {
                bs1.shutdown();
            }
        }

        // Multiple commas
        userString = "jerrySeinfeld,,,elaineBennis,,peterman234," + System.getProperty("user.name") + ",puddy";
        conf.setPermittedStartupUsers(userString);
        try {
            bs1 = new BookieServer(conf);
            bs1.start();
        } catch (AccessControlException buae) {
            Assert.fail("Bookkeeper should have started since current user is in permittedStartupUsers");
        } finally {
            if (bs1 != null && bs1.isRunning()) {
                bs1.shutdown();
            }
        }

        // Comma at end
        userString = "jerrySeinfeld,elaineBennis,peterman234," + System.getProperty("user.name") + ",puddy,";
        conf.setPermittedStartupUsers(userString);
        try {
            bs1 = new BookieServer(conf);
            bs1.start();
        } catch (AccessControlException buae) {
            Assert.fail("Bookkeeper should have started since current user is in permittedStartupUsers");
        } finally {
            if (bs1 != null && bs1.isRunning()) {
                bs1.shutdown();
            }
        }
    }

    /**
     * Verify user can start if user is not in permittedStartupUsers but it is empty BKException
     * BKUnauthorizedAccessException if cannot start
     */
    @Test(timeout = 20000)
    public void testUserPermittedToStartWithEmptyProperty() throws Exception {
        File tmpDir = createTempDir("bookie", "test");

        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        int port = 12555;
        conf.setZkServers(null).setBookiePort(port).setJournalDirName(tmpDir.getPath())
                .setLedgerDirNames(new String[] { tmpDir.getPath() });
        conf.setPermittedStartupUsers(" ");
        BookieServer bs1 = null;
        try {
            bs1 = new BookieServer(conf);
            bs1.start();
        } catch (AccessControlException buae) {
            Assert.fail("Bookkeeper should have started since current user is in permittedStartupUsers");
        } finally {
            if (bs1 != null && bs1.isRunning()) {
                bs1.shutdown();
            }
        }
    }

    /**
     * Verify duplicate bookie server startup. Should throw
     * java.net.BindException if already BK server is running
     */
    @Test(timeout = 20000)
    public void testDuplicateBookieServerStartup() throws Exception {
        File tmpDir = createTempDir("bookie", "test");

        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        int port = 12555;
        conf.setZkServers(null).setBookiePort(port).setJournalDirName(
                tmpDir.getPath()).setLedgerDirNames(
                new String[] { tmpDir.getPath() });
        BookieServer bs1 = new BookieServer(conf);
        bs1.start();

        // starting bk server with same conf
        try {
            BookieServer bs2 = new BookieServer(conf);
            bs2.start();
            fail("Should throw BindException, as the bk server is already running!");
        } catch (BindException e) {
            // Ok
        } catch (IOException e) {
            Assert.assertTrue("BKServer allowed duplicate Startups!",
                    e.getMessage().contains("bind"));
        }
        finally {
            if (bs1 != null && bs1.isRunning()) {
                bs1.shutdown();
            }
        }
    }

    /**
     * Verify bookie start behaviour when ZK Server is not running.
     */
    @Test(timeout = 20000)
    public void testStartBookieWithoutZKServer() throws Exception {
        zkUtil.killServer();

        File tmpDir = createTempDir("bookie", "test");

        final ServerConfiguration conf = TestBKConfiguration.newServerConfiguration()
                .setZkServers(zkUtil.getZooKeeperConnectString())
                .setZkTimeout(5000).setJournalDirName(tmpDir.getPath())
                .setLedgerDirNames(new String[] { tmpDir.getPath() });
        try {
            new Bookie(conf);
            fail("Should throw ConnectionLossException as ZKServer is not running!");
        } catch (KeeperException.ConnectionLossException e) {
            // expected behaviour
        }
    }

    /**
     * Verify that if I try to start a bookie without zk initialized, it won't
     * prevent me from starting the bookie when zk is initialized
     */
    @Test(timeout = 20000)
    public void testStartBookieWithoutZKInitialized() throws Exception {
        File tmpDir = createTempDir("bookie", "test");
        final String ZK_ROOT = "/ledgers2";

        final ServerConfiguration conf = TestBKConfiguration.newServerConfiguration()
            .setZkServers(zkUtil.getZooKeeperConnectString())
            .setZkTimeout(5000).setJournalDirName(tmpDir.getPath())
            .setLedgerDirNames(new String[] { tmpDir.getPath() });
        conf.setZkLedgersRootPath(ZK_ROOT);
        try {
            new Bookie(conf);
            fail("Should throw NoNodeException");
        } catch (Exception e) {
            // shouldn't be able to start
        }
        ClientConfiguration clientConf = new ClientConfiguration();
        clientConf.setZkServers(zkUtil.getZooKeeperConnectString());
        clientConf.setZkLedgersRootPath(ZK_ROOT);
        BookKeeperAdmin.format(clientConf, false, false);

        Bookie b = new Bookie(conf);
        b.shutdown();
    }

    /**
     * Check disk full. Expected to fail on start.
     */
    @Test(timeout = 30000)
    public void testWithDiskFullReadOnlyDisabledOrForceGCAllowDisabled() throws Exception {
        File tmpDir = createTempDir("DiskCheck", "test");
        long usableSpace = tmpDir.getUsableSpace();
        long totalSpace = tmpDir.getTotalSpace();
        final ServerConfiguration conf = TestBKConfiguration.newServerConfiguration()
                .setZkServers(zkUtil.getZooKeeperConnectString())
                .setZkTimeout(5000).setJournalDirName(tmpDir.getPath())
                .setLedgerDirNames(new String[] { tmpDir.getPath() })
                .setDiskCheckInterval(1000)
                .setDiskUsageThreshold((1.0f - ((float) usableSpace / (float) totalSpace)) * 0.999f)
                .setDiskUsageWarnThreshold(0.0f);
        
        // if isForceGCAllowWhenNoSpace or readOnlyModeEnabled is not set and Bookie is 
        // started when Disk is full, then it will fail to start with NoWritableLedgerDirException
        
        conf.setIsForceGCAllowWhenNoSpace(false)
            .setReadOnlyModeEnabled(false);
        try {
            new Bookie(conf);
            fail("NoWritableLedgerDirException expected");
        } catch(NoWritableLedgerDirException e) {
            // expected
        }
        
        conf.setIsForceGCAllowWhenNoSpace(true)
            .setReadOnlyModeEnabled(false);
        try {
            new Bookie(conf);
            fail("NoWritableLedgerDirException expected");
        } catch(NoWritableLedgerDirException e) {
            // expected
        }
    }
    
    /**
     * Check disk full. Expected to start as read-only.
     */
    @Test(timeout = 30000)
    public void testWithDiskFullReadOnlyEnabledAndForceGCAllowAllowed() throws Exception {
        File tmpDir = createTempDir("DiskCheck", "test");
        long usableSpace = tmpDir.getUsableSpace();
        long totalSpace = tmpDir.getTotalSpace();
        final ServerConfiguration conf = TestBKConfiguration.newServerConfiguration()
                .setZkServers(zkUtil.getZooKeeperConnectString())
                .setZkTimeout(5000).setJournalDirName(tmpDir.getPath())
                .setLedgerDirNames(new String[] { tmpDir.getPath() })
                .setDiskCheckInterval(1000)
                .setDiskUsageThreshold((1.0f - ((float) usableSpace / (float) totalSpace)) * 0.999f)
                .setDiskUsageWarnThreshold(0.0f);
        
        // if isForceGCAllowWhenNoSpace and readOnlyModeEnabled are set, then Bookie should
        // start with readonlymode when Disk is full (assuming there is no need for creation of index file
        // while replaying the journal)
        conf.setReadOnlyModeEnabled(true)
            .setIsForceGCAllowWhenNoSpace(true);
        final Bookie bk = new Bookie(conf);
        bk.start();
        Thread.sleep((conf.getDiskCheckInterval() * 2) + 100);
        
        assertTrue(bk.isReadOnly());
        bk.shutdown();
    }

    class MockBookieServer extends BookieServer {
        ServerConfiguration conf;

        public MockBookieServer(ServerConfiguration conf) throws IOException, KeeperException, InterruptedException,
                BookieException, UnavailableException, CompatibilityException, SecurityException {
            super(conf);
            this.conf = conf;
        }

        @Override
        protected Bookie newBookie(ServerConfiguration conf)
                throws IOException, KeeperException, InterruptedException, BookieException {
            return new MockBookieWithNoopShutdown(conf, NullStatsLogger.INSTANCE);
        }
    }

    class MockBookieWithNoopShutdown extends Bookie {
        public MockBookieWithNoopShutdown(ServerConfiguration conf, StatsLogger statsLogger)
                throws IOException, KeeperException, InterruptedException, BookieException {
            super(conf, statsLogger);
        }

        // making Bookie Shutdown no-op. Ideally for this testcase we need to
        // kill bookie abruptly to simulate the scenario where bookie is killed
        // without execution of shutdownhook (and corresponding shutdown logic).
        // Since there is no easy way to simulate abrupt kill of Bookie we are
        // injecting noop Bookie Shutdown
        @Override
        synchronized int shutdown(int exitCode) {
            return exitCode;
        }
    }
    
    @Test(timeout = 30000)
    public void testWithDiskFullAndAbilityToCreateNewIndexFile() throws Exception {
        File tmpDir = createTempDir("DiskCheck", "test");

        final ServerConfiguration conf = TestBKConfiguration.newServerConfiguration()
                .setZkServers(zkUtil.getZooKeeperConnectString()).setZkTimeout(5000).setJournalDirName(tmpDir.getPath())
                .setLedgerDirNames(new String[] { tmpDir.getPath() }).setDiskCheckInterval(1000)
                .setLedgerStorageClass(SortedLedgerStorage.class.getName()).setAutoRecoveryDaemonEnabled(false);

        BookieServer server = new MockBookieServer(conf);
        server.start();
        ClientConfiguration clientConf = new ClientConfiguration();
        clientConf.setZkServers(zkUtil.getZooKeeperConnectString());
        BookKeeper bkClient = new BookKeeper(clientConf);
        LedgerHandle lh = bkClient.createLedger(1, 1, 1, DigestType.CRC32, "passwd".getBytes());
        long entryId = -1;
        long numOfEntries = 5;
        for (int i = 0; i < numOfEntries; i++) {
            entryId = lh.addEntry("data".getBytes());
        }
        Assert.assertTrue("EntryId of the recently added entry should be 0", entryId == (numOfEntries - 1));
        // We want to simulate the scenario where Bookie is killed abruptly, so
        // SortedLedgerStorage's EntryMemTable and IndexInMemoryPageManager are
        // not flushed and hence when bookie is restarted it will replay the
        // journal. Since there is no easy way to kill the Bookie abruptly, we
        // are injecting no-op shutdown.
        server.shutdown();

        long usableSpace = tmpDir.getUsableSpace();
        long totalSpace = tmpDir.getTotalSpace();
        conf.setDiskUsageThreshold(0.001f)
                .setDiskUsageWarnThreshold(0.0f).setReadOnlyModeEnabled(true).setIsForceGCAllowWhenNoSpace(true)
                .setMinUsableSizeForIndexFileCreation(Long.MAX_VALUE);
        server = new BookieServer(conf);
        // Now we are trying to start the Bookie, which tries to replay the
        // Journal. While replaying the Journal it tries to create the IndexFile
        // for the ledger (whose entries are not flushed). but since we set
        // minUsableSizeForIndexFileCreation to very high value, it wouldn't. be
        // able to find any index dir when all discs are full
        server.start();
        Assert.assertFalse("Bookie should be Shutdown", server.getBookie().isRunning());
        server.shutdown();

        // Here we are setting MinUsableSizeForIndexFileCreation to very low
        // value. So if index dirs are full then it will consider the dirs which
        // have atleast MinUsableSizeForIndexFileCreation usable space for the
        // creation of new Index file.
        conf.setMinUsableSizeForIndexFileCreation(5 * 1024);
        server = new BookieServer(conf);
        server.start();
        Thread.sleep((conf.getDiskCheckInterval() * 2) + 100);
        Assert.assertTrue("Bookie should be up and running", server.getBookie().isRunning());
        assertTrue(server.getBookie().isReadOnly());
        server.shutdown();
        bkClient.close();
    }
    
    static class MockInterleavedLedgerStorage extends InterleavedLedgerStorage {
        @Override
        public void shutdown() {
            // During BookieServer shutdown this method will be called 
            // and we want it to be noop.
            // do nothing
        }
        
        @Override
        synchronized public void flush() throws IOException {
            // this method will be called by SyncThread.shutdown.
            // During BookieServer shutdown we want this method to be noop
            // do nothing
        }
    }
    
    @Test(timeout = 30000)
    public void testCheckPointForEntryLoggerWithMultipleActiveEntryLogs() throws Exception {
        File tmpDir = createTempDir("DiskCheck", "test");

        final ServerConfiguration conf = TestBKConfiguration.newServerConfiguration()
                .setZkServers(zkUtil.getZooKeeperConnectString())
                .setZkTimeout(5000)
                .setJournalDirName(tmpDir.getPath())
                .setLedgerDirNames(new String[] { tmpDir.getPath() })
                .setAutoRecoveryDaemonEnabled(false)
                .setFlushInterval(5000)
                .setBookiePort(PortManager.nextFreePort())
                // entrylog per ledger is enabled
                .setEntryLogPerLedgerEnabled(true)
                .setLedgerStorageClass(MockInterleavedLedgerStorage.class.getName());
        // we know there is only one ledgerDir
        File ledgerDir = Bookie.getCurrentDirectories(conf.getLedgerDirs())[0];
        BookieServer server = new BookieServer(conf);
        server.start();
        ClientConfiguration clientConf = new ClientConfiguration();
        clientConf.setZkServers(zkUtil.getZooKeeperConnectString());
        BookKeeper bkClient = new BookKeeper(clientConf);
 
        ExecutorService threadPool = Executors.newFixedThreadPool(30);
        int numOfLedgers = 12;
        int numOfEntries = 500;
        byte[] dataBytes = "data".getBytes();
        LedgerHandle[] handles = new LedgerHandle[numOfLedgers];
        AtomicBoolean receivedExceptionForAdd = new AtomicBoolean(false);
        CountDownLatch countDownLatch = new CountDownLatch(numOfLedgers * numOfEntries);
        for (int i = 0; i < numOfLedgers; i++) {
            int ledgerIndex = i;
            handles[i] = bkClient.createLedgerAdv((long) i, 1, 1, 1, DigestType.CRC32, "passwd".getBytes(), null);
            for (int j = 0; j < numOfEntries; j++) {
                int entryIndex = j;
                threadPool.submit(() -> {
                    try {
                        handles[ledgerIndex].addEntry(entryIndex, dataBytes);
                    } catch (Exception e) {
                        LOG.error("Got Exception while trying to addEntry for ledgerId: " + ledgerIndex + " entry: "
                                + entryIndex, e);
                        receivedExceptionForAdd.set(true);
                    } finally {
                        countDownLatch.countDown();
                    }
                });
            }
        }
        Assert.assertTrue("It is expected add requests are supposed to be completed in 3000 secs",
                countDownLatch.await(3000, TimeUnit.MILLISECONDS));
        Assert.assertFalse("there shouldn't be any exceptions for addentry requests", receivedExceptionForAdd.get());
        for (int i = 0; i < numOfLedgers; i++) {
            handles[i].close();
        }
        threadPool.shutdown();
        
        LastLogMark lastLogMarkBeforeCheckpoint = server.getBookie().journal.getLastLogMark();
        LogMark curMarkBeforeCheckpoint = lastLogMarkBeforeCheckpoint.getCurMark();

        File lastMarkFile = new File(ledgerDir, "lastMark");
        // lastMark file should not be existing, because checkpoint has not happened so far
        Assert.assertFalse("lastMark file should not be existing, because checkpoint has not happened so far",
                lastMarkFile.exists());
        Thread.sleep(conf.getFlushInterval() + 1000);
        // since we have waited for more than flushInterval SyncThread should have checkpointed.
        // if entrylogperledger is not enabled, then we checkpoint only when currentLog in EntryLogger
        // is rotated. but if entrylogperledger is enabled, then we checkpoint for every flushInterval period
        Assert.assertTrue("lastMark file must be existing, because checkpoint should have happened",
                lastMarkFile.exists());
        
        LastLogMark lastLogMarkAfterCheckpoint = server.getBookie().journal.getLastLogMark();
        LogMark curMarkAfterCheckpoint = lastLogMarkAfterCheckpoint.getCurMark();
        
        byte buff[] = new byte[16];
        ByteBuffer bb = ByteBuffer.wrap(buff);
        LogMark rolledLogMark = new LogMark();
        FileInputStream fis = new FileInputStream(lastMarkFile);
        int bytesRead = fis.read(buff);
        fis.close();
        if (bytesRead != 16) {
            throw new IOException("Couldn't read enough bytes from lastMark."
                                  + " Wanted " + 16 + ", got " + bytesRead);
        }
        bb.clear();
        rolledLogMark.readLogMark(bb);
        // Curmark should be equal before and after checkpoint,
        // because we didnt add new entries during this period
        Assert.assertTrue("Curmark should be equal before and after checkpoint",
                curMarkAfterCheckpoint.compare(curMarkBeforeCheckpoint) == 0);
        // Curmark after checkpoint should be equal to rolled logmark,
        // because we checkpointed 
        Assert.assertTrue("Curmark after checkpoint should be equal to rolled logmark",
                curMarkAfterCheckpoint.compare(rolledLogMark) == 0);

        // here we are calling shutdown, but MockInterleavedLedgerStorage shudown/flush 
        // methods are noop, so entrylogger is not flushed as part of this shutdown
        // here we are trying to simulate Bookie crash, but there is no way to
        // simulate bookie abrupt crash
        server.shutdown();
        bkClient.close();
        
        // delete journal files and lastMark, to make sure that we are not reading from
        // Journal file
        File journalDirectory = Bookie.getCurrentDirectory(conf.getJournalDir());
        List<Long> journalLogsId = Journal.listJournalIds(journalDirectory, null);
        for (long journalId : journalLogsId) {
            File journalFile = new File(journalDirectory, Long.toHexString(journalId) + ".txn");
            journalFile.delete();
        }
        
        // we know there is only one ledgerDir
        lastMarkFile = new File(ledgerDir, "lastMark");
        lastMarkFile.delete();
        
        // now we are restarting BookieServer
        server = new BookieServer(conf);
        server.start();
        bkClient = new BookKeeper(clientConf);
        // since Bookie checkpointed successfully before shutdown/crash, 
        // we should be able to read from entryLogs though journal is deleted
        for (int i = 0; i < numOfLedgers; i++) {
            LedgerHandle lh = bkClient.openLedger(i, DigestType.CRC32, "passwd".getBytes());
            Enumeration<LedgerEntry> entries = lh.readEntries(0, numOfEntries - 1);
            while (entries.hasMoreElements()) {
                LedgerEntry entry = entries.nextElement();
                byte[] readData = entry.getEntry();
                Assert.assertEquals("Ledger Entry Data should match", new String("data".getBytes()),
                        new String(readData));
            }
        }
        server.shutdown();
    }

    /**
     * Check disk error for file. Expected to throw DiskErrorException.
     */
    @Test(timeout = 30000)
    public void testWithDiskError() throws Exception {
        File parent = createTempDir("DiskCheck", "test");
        File child = File.createTempFile("DiskCheck", "test", parent);
        final ServerConfiguration conf = TestBKConfiguration.newServerConfiguration()
                .setZkServers(zkUtil.getZooKeeperConnectString())
                .setZkTimeout(5000).setJournalDirName(child.getPath())
                .setLedgerDirNames(new String[] { child.getPath() });
        try {
            // LedgerDirsManager#init() is used in Bookie instantiation.
            // Simulating disk errors by directly calling #init
            LedgerDirsManager ldm = new LedgerDirsManager(conf, conf.getLedgerDirs(),
                    new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold()));
            LedgerDirsMonitor ledgerMonitor = new LedgerDirsMonitor(conf, 
                    new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold()), ldm);
            ledgerMonitor.init();
            fail("should throw exception");
        } catch (Exception e) {
            // expected
        }
    }
    
    /**
     * if ALLOW_DIRS_PARTITION_DUPLICATION is disabled then Bookie initialization
     * will fail if there are multiple ledger/index dirs are in same partition/filesystem.
     */
    @Test(timeout = 2000000)
    public void testAllowDirsPartitionDuplicationDisabled() throws Exception {
        File tmpDir1 = createTempDir("bookie", "test");
        File tmpDir2 = createTempDir("bookie", "test");

        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        int port = 12555;
        conf.setZkServers(zkUtil.getZooKeeperConnectString()).setZkTimeout(5000).setBookiePort(port)
                .setJournalDirName(tmpDir1.getPath())
                .setLedgerDirNames(new String[] { tmpDir1.getPath(), tmpDir2.getPath() });
        conf.setAllowMultipleDirsUnderSamePartition(false);
        BookieServer bs1 = null;
        try {
            bs1 = new BookieServer(conf);
            Assert.fail("Bookkeeper should not have started since AllowDirsPartitionDuplication is not enabled");
        } catch (DirsPartitionDuplicationException dpde) {
            // Expected
        } finally {
            if (bs1 != null) {
                bs1.shutdown();
            }
        }
    }
    
    /**
     * if ALLOW_DIRS_PARTITION_DUPLICATION is enabled then Bookie initialization
     * should succeed even if there are multiple ledger/index dirs in the same partition/filesystem.
     */
    @Test(timeout = 2000000)
    public void testAllowDirsPartitionDuplicationAllowed() throws Exception {
        File tmpDir1 = createTempDir("bookie", "test");
        File tmpDir2 = createTempDir("bookie", "test");

        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        int port = 12555;
        conf.setZkServers(zkUtil.getZooKeeperConnectString()).setZkTimeout(5000).setBookiePort(port)
                .setJournalDirName(tmpDir1.getPath())
                .setLedgerDirNames(new String[] { tmpDir1.getPath(), tmpDir2.getPath() });
        conf.setAllowMultipleDirsUnderSamePartition(true);
        BookieServer bs1 = null;
        try {
            bs1 = new BookieServer(conf);          
        } catch (DirsPartitionDuplicationException dpde) {
            Assert.fail("Bookkeeper should have started since AllowDirsPartitionDuplication is enabled");
        } finally {
            if (bs1 != null) {
                bs1.shutdown();
            }
        }
    }
    
    private ZooKeeperClient createNewZKClient() throws Exception {
        // create a zookeeper client
        LOG.debug("Instantiate ZK Client");
        return ZooKeeperClient.newBuilder()
                .connectString(zkUtil.getZooKeeperConnectString())
                .build();
    }

    @Test(timeout = 20000)
    public void testJettyEndpointStart() throws Exception {
        File tmpDir = createTempDir("bookie", "test");

        final ServerConfiguration conf = TestBKConfiguration.newServerConfiguration()
                .setJournalDirName(tmpDir.getPath()).setLedgerDirNames(new String[] { tmpDir.getPath() })
                .setBookiePort(PortManager.nextFreePort());

        String statServletContext = "/stats";
        String statEndpoint = "/metrics.json";
        int nextFreePort = PortManager.nextFreePort();
        conf.setJettyPort(nextFreePort);
        conf.setStatsEnabled(true);
        conf.setEnableRestEndpoints(true);
        conf.setStatServletContext(statServletContext);
        conf.setRestServletContext("/rest");
        conf.setRestPackage("org.apache.bookkeeper.util");
        conf.setStatServletEndpoint(statEndpoint);
        conf.setStatsProviderClass(org.apache.bookkeeper.stats.CodahaleMetricsProvider.class);
        String urlAddr = "http://localhost:" + conf.getJettyPort() + conf.getStatServletContext()
                + conf.getStatServletEndpoint();
        Class<? extends StatsProvider> statsProviderClass = conf.getStatsProviderClass();
        final StatsProvider statsProvider = ReflectionUtils.newInstance(statsProviderClass);
        statsProvider.start(conf);
        BookieServer bkServer = new BookieServer(conf, statsProvider.getStatsLogger(""));
        bkServer.start();
        URL url = new URL(urlAddr);
        @SuppressWarnings("unchecked")
        Map<String, Object> statMap = om.readValue(url, Map.class);
        if (statMap.isEmpty() || !statMap.containsKey("counters")) {
            Assert.fail("Failed to map metrics to valid JSON entries on servlet.");
        }
        // Now, hit the rest endpoint we know exists to ensure Jersey servlet is
        // up
        url = new URL("http://localhost:" + conf.getJettyPort() + conf.getRestServletContext() +
                "/resources/v1/configurations");
        @SuppressWarnings("unchecked")
        Map<String, Object> configMap = om.readValue(url, Map.class);
        if (configMap.isEmpty() || !(configMap.containsKey("jettyPort") && configMap.containsKey("bookiePort"))) {
            Assert.fail("Failed to map /rest/resources/v1/configurations to valid JSON entries.");
        }
        if (bkServer.isRunning()) {
            bkServer.shutdown();
        }
    }

    @Test (timeout = 20000)
    public void testJettyEndpointStartOneOnOneOff() throws Exception {
        File tmpDir = createTempDir("bookie", "test");

        final ServerConfiguration conf = TestBKConfiguration.newServerConfiguration()
                .setJournalDirName(tmpDir.getPath()).setLedgerDirNames(new String[] { tmpDir.getPath() })
                .setBookiePort(PortManager.nextFreePort());
        String statServletContext = "/stats";
        String statEndpoint = "/metrics.json";
        int nextFreePort = PortManager.nextFreePort();
        conf.setJettyPort(nextFreePort);
        conf.setStatsEnabled(true);
        conf.setRestServletContext("/rest");
        conf.setEnableRestEndpoints(false);
        conf.setRestPackage("org.apache.bookkeeper.util");
        conf.setStatServletContext(statServletContext);
        conf.setStatServletEndpoint(statEndpoint);
        conf.setStatsProviderClass(org.apache.bookkeeper.stats.CodahaleMetricsProvider.class);
        String urlAddr = "http://localhost:" + conf.getJettyPort() + conf.getStatServletContext()
        + conf.getStatServletEndpoint();
        Class<? extends StatsProvider> statsProviderClass = conf.getStatsProviderClass();
        final StatsProvider statsProvider = ReflectionUtils.newInstance(statsProviderClass);
        statsProvider.start(conf);
        BookieServer bkServer = new BookieServer(conf, statsProvider.getStatsLogger(""));
        bkServer.start();
        URL url = new URL(urlAddr);
        @SuppressWarnings("unchecked")
        Map<String, Object> statMap = om.readValue(url, Map.class);
        if (statMap.isEmpty() || !statMap.containsKey("counters")) {
            Assert.fail("Failed to map metrics to valid JSON entries on servlet.");
        }
        // Now, hit the rest endpoint we know exists to ensure Jersey servlet is
        // up
        url = new URL("http://localhost:" + conf.getJettyPort() + conf.getRestServletContext()
                + "/resources/v1/configurations");
        try{
            @SuppressWarnings("unchecked")
            Map<String,Object> configMap = om.readValue(url, Map.class);
            Assert.fail("Endpoint for REST shouldn't be up. Failing.");
        }
        catch (FileNotFoundException fe) {
            //Good... doesn't exist. 
        }
        if (bkServer.isRunning()) {
            bkServer.shutdown();
        }
    }
}
