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
import java.io.IOException;
import java.net.BindException;
import java.net.InetAddress;
import java.security.AccessControlException;

import org.apache.bookkeeper.bookie.BookieException.BookieUnauthorizedAccessException;
import org.apache.bookkeeper.bookie.LedgerDirsManager.NoWritableLedgerDirException;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.util.DiskChecker;
import org.apache.bookkeeper.zookeeper.ZooKeeperClient;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.jboss.netty.channel.ChannelException;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Testing bookie initialization cases
 */
public class BookieInitializationTest extends BookKeeperClusterTestCase {
    private static final Logger LOG = LoggerFactory
            .getLogger(BookieInitializationTest.class);

    ZooKeeper newzk = null;

    public BookieInitializationTest() {
        super(0);
    }

    @Override
    public void tearDown() throws Exception {
        if (null != newzk) {
            newzk.close();
        }
        super.tearDown();
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
        createNewZKClient();
        b.zk = newzk;

        // deleting the znode, so that the bookie registration should
        // continue successfully on NodeDeleted event
        new Thread() {
            @Override
            public void run() {
                try {
                    Thread.sleep(conf.getZkTimeout() / 3);
                    zkc.delete(bkRegPath, -1);
                } catch (Exception e) {
                    // Not handling, since the testRegisterBookie will fail
                    LOG.error("Failed to delete the znode :" + bkRegPath, e);
                }
            }
        }.start();
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
        Stat bkRegNode2 = newzk.exists(bkRegPath, false);
        Assert.assertNotNull("Bookie registration has been failed", bkRegNode2);
        Assert.assertTrue("Bookie is referring to old registration znode:"
                + bkRegNode1 + ", New ZNode:" + bkRegNode2, bkRegNode1
                .getEphemeralOwner() != bkRegNode2.getEphemeralOwner());
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
        createNewZKClient();
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
        }
    }

	/**
	 * Verify user cannot start if user is in permittedStartupUsers conf list
	 * BKException BKUnauthorizedAccessException if cannot start
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
		}
	}

	/**
	 * Verify user cannot start if user is in permittedStartupUsers conf list
	 * BKException BKUnauthorizedAccessException if cannot start
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
	 * Verify user can start if user is not in permittedStartupUsers but it is
	 * empty BKException BKUnauthorizedAccessException if cannot start
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
        } catch (ChannelException ce) {
            Assert.assertTrue("Should be caused by a bind exception",
                              ce.getCause() instanceof BindException);            
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
    public void testWithDiskFullReadOnlyDisabled() throws Exception {
        File tmpDir = createTempDir("DiskCheck", "test");
        long usableSpace = tmpDir.getUsableSpace();
        long totalSpace = tmpDir.getTotalSpace();
        final ServerConfiguration conf = TestBKConfiguration.newServerConfiguration()
                .setZkServers(zkUtil.getZooKeeperConnectString())
                .setZkTimeout(5000).setJournalDirName(tmpDir.getPath())
                .setLedgerDirNames(new String[] { tmpDir.getPath() })
                .setDiskCheckInterval(1000)
                .setReadOnlyModeEnabled(false);
        conf.setDiskUsageThreshold((1.0f - ((float) usableSpace / (float) totalSpace)) * 0.999f);
        conf.setDiskUsageWarnThreshold(0.0f);
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
    public void testWithDiskFullReadOnlyEnabled() throws Exception {
        File tmpDir = createTempDir("DiskCheck", "test");
        long usableSpace = tmpDir.getUsableSpace();
        long totalSpace = tmpDir.getTotalSpace();
        final ServerConfiguration conf = TestBKConfiguration.newServerConfiguration()
                .setZkServers(zkUtil.getZooKeeperConnectString())
                .setZkTimeout(5000).setJournalDirName(tmpDir.getPath())
                .setLedgerDirNames(new String[] { tmpDir.getPath() })
                .setDiskCheckInterval(1000)
                .setReadOnlyModeEnabled(true);
        conf.setDiskUsageThreshold((1.0f - ((float) usableSpace / (float) totalSpace)) * 0.999f);
        conf.setDiskUsageWarnThreshold(0.0f);
        final Bookie bk = new Bookie(conf);
        Thread.sleep((conf.getDiskCheckInterval() * 2) + 100);
        
        assertTrue(bk.isReadOnly());
        bk.shutdown();
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
            LedgerDirsManager ldm = new LedgerDirsManager(conf, conf.getLedgerDirs());
            LedgerDirsMonitor ledgerMonitor = new LedgerDirsMonitor(conf, 
                    new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold()), ldm);
            ledgerMonitor.init();
            fail("should throw exception");
        } catch (Exception e) {
            // expected
        }
    }

    private void createNewZKClient() throws Exception {
        // create a zookeeper client
        LOG.debug("Instantiate ZK Client");
        newzk = ZooKeeperClient.newBuilder()
                .connectString(zkUtil.getZooKeeperConnectString())
                .build();
    }
}
