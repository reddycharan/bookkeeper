package org.apache.bookkeeper.proxyclient;

import java.io.IOException;
import java.security.AccessControlException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.bookkeeper.BKPConstants;
import org.apache.bookkeeper.BKProxyMain;
import org.apache.bookkeeper.conf.BookKeeperProxyConfiguration;
import org.apache.bookkeeper.util.LedgerIdFormatter.UUIDLedgerIdFormatter;
import org.apache.bookkeeper.meta.LongHierarchicalLedgerManagerFactory;
import org.apache.bookkeeper.proxyclient.ReturnValue.AsyncWriteStatusReturnValue;
import org.apache.bookkeeper.proxyclient.ReturnValue.CreateExtentReturnValue;
import org.apache.bookkeeper.proxyclient.ReturnValue.ExtentListGetReturnValue;
import org.apache.bookkeeper.proxyclient.ReturnValue.ExtentStatReturnValue;
import org.apache.bookkeeper.proxyclient.ReturnValue.ReadFragmentReturnValue;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class BKProxyClientTest extends BookKeeperClusterTestCase {

    private BookKeeperProxyConfiguration commonBKProxyConfig;
    private HashMap<Integer, BKProxyMain> bkProxiesMap;

    public BKProxyClientTest() {
        super(3);
        baseConf.setLedgerManagerFactoryClass(LongHierarchicalLedgerManagerFactory.class);
        baseClientConf.setLedgerManagerFactoryClass(LongHierarchicalLedgerManagerFactory.class);
    }

    @Before
    public void testcaseSetup() throws InterruptedException {
        commonBKProxyConfig = new BookKeeperProxyConfiguration();
        commonBKProxyConfig.setZkServers(zkUtil.getZooKeeperConnectString());
        commonBKProxyConfig.setLedgerIdFormatterClass(UUIDLedgerIdFormatter.class);
        bkProxiesMap = new HashMap<Integer, BKProxyMain>();
    }

    @After
    public void testcaseCleanup() {
        shutDownAllBkProxies();
    }

    public void addAndStartBKP(int bkProxyPort) throws InterruptedException {
        BookKeeperProxyConfiguration thisBKPConfig = new BookKeeperProxyConfiguration(commonBKProxyConfig);
        thisBKPConfig.setBKProxyPort(bkProxyPort);
        BKProxyMain bkProxy = new BKProxyMain(thisBKPConfig);
        Thread bkProxyThread = new Thread(bkProxy);
        bkProxyThread.start();
        Thread.sleep(2000);
        bkProxiesMap.put(bkProxyPort, bkProxy);
    }

    public void shutDownAllBkProxies() {
        for (Integer bkProxyPort : bkProxiesMap.keySet()) {
            BKProxyMain bkProxy = bkProxiesMap.get(bkProxyPort);
            bkProxy.shutdown();
        }
    }

    @Test(timeout = 60000)
    public void basicTestWithExtentCreateAndClose() throws IOException, InterruptedException {
        addAndStartBKP(5555);
        BKProxyClient proxyClient = new BKProxyClient("localhost", 5555);
        proxyClient.init();
        CreateExtentReturnValue createExtentReturnValue = proxyClient.createExtent();
        Assert.assertTrue("Extent should be created successfully",
                createExtentReturnValue.getReturnCode() == BKPConstants.SF_OK);
        byte[] extentId = createExtentReturnValue.getExtentId();
        proxyClient.writeFragment(extentId, 1, "hello".getBytes());
        proxyClient.writeFragment(extentId, 2, "world".getBytes());
        ReadFragmentReturnValue readFragmentReturnValue = proxyClient.readFragment(extentId, 1);
        Assert.assertTrue("Fragment should be created successfully",
                readFragmentReturnValue.getReturnCode() == BKPConstants.SF_OK);
        Assert.assertTrue("Fragment data should match exactly",
                new String(readFragmentReturnValue.getFragmentData()).equals("hello"));
        proxyClient.writeCloseExtent(extentId);

        ExtentStatReturnValue extentStatReturnValue = proxyClient.getExtentStat(extentId);
        Assert.assertTrue("Length of Extent should match",
                extentStatReturnValue.getExtentLength() == "helloworld".length());

        proxyClient.close();
    }

    @Test(timeout = 60000)
    public void extentListTest() throws IOException, InterruptedException {
        addAndStartBKP(5555);
        BKProxyClient proxyClient = new BKProxyClient("localhost", 5555);
        proxyClient.init();
        int numOfLedgers = BKPConstants.LEDGER_LIST_BATCH_SIZE + 5;
        CreateExtentReturnValue createExtentReturnValue;
        List<PrimitiveByteArrayWrapper> createdExtentIdsWrapperList = new ArrayList<PrimitiveByteArrayWrapper>();
        for (int i = 0; i < numOfLedgers; i++) {
            createExtentReturnValue = proxyClient.createExtent();
            byte[] extentId = createExtentReturnValue.getExtentId();
            createdExtentIdsWrapperList.add(new PrimitiveByteArrayWrapper(extentId));
            proxyClient.writeCloseExtent(extentId);
        }

        ExtentListGetReturnValue extentListGetReturnValue = proxyClient.getExtentsList();
        List<byte[]> extentIdsList = extentListGetReturnValue.getExtentIdsList();

        List<PrimitiveByteArrayWrapper> extentIdsListWrapperList = extentIdsList.stream()
                .map((extentIdByteArray) -> new PrimitiveByteArrayWrapper(extentIdByteArray))
                .collect(Collectors.toList());

        Assert.assertTrue("ExtentIds list should match",
                extentIdsListWrapperList.containsAll(createdExtentIdsWrapperList)
                        && createdExtentIdsWrapperList.containsAll(extentIdsListWrapperList));
        proxyClient.close();
    }

	@Test(timeout = 60000)
	public void testUserPermittedToStart() {
		String userString = "";
		BookKeeperProxyConfiguration thisBKPConfig = new BookKeeperProxyConfiguration(commonBKProxyConfig);
		thisBKPConfig.setBKProxyPort(5555);
		try {
			userString = "jerrySeinfeld,elaineBennis,kramer,soupNazi, " + System.getProperty("user.name");
			thisBKPConfig.setPermittedStartupUsers(userString);
			BKProxyMain bkProxy = new BKProxyMain(thisBKPConfig);
		} catch (AccessControlException ace) {
			Assert.fail("Current user is in permittedStartupUsers, but startup failed.");
		}
	}

	@Test(timeout = 60000)
	public void testUserNotPermittedToStart() {
		try {
			BookKeeperProxyConfiguration thisBKPConfig = new BookKeeperProxyConfiguration(commonBKProxyConfig);
			thisBKPConfig.setBKProxyPort(5555);
			String userString = "jerrySeinfeld,elaineBennis,kramer,soupNazi";
			thisBKPConfig.setPermittedStartupUsers(userString);
			BKProxyMain bkProxy = new BKProxyMain(thisBKPConfig);
			Assert.fail("Current user started process but was not approved to." + "Current user: "
					+ System.getProperty("user.name") + "\t Users: " + userString);
		} catch (AccessControlException ace) {
			// Expected! Success!
		}
	}

    @Test(timeout = 60000)
    public void fragmentsWriteReadTest() throws IOException, InterruptedException {
        addAndStartBKP(5555);
        BKProxyClient proxyClient = new BKProxyClient("localhost", 5555);
        proxyClient.init();
        CreateExtentReturnValue createExtentReturnValue = proxyClient.createExtent();
        Assert.assertTrue("Extent should be created successfully",
                createExtentReturnValue.getReturnCode() == BKPConstants.SF_OK);
        byte[] extentId = createExtentReturnValue.getExtentId();
        int numOfFragments = 10;
        for (int i = 1; i <= numOfFragments; i++) {
            ReturnValue returnValue = proxyClient.writeFragment(extentId, i, ("fragment" + i).getBytes());
            Assert.assertTrue("WriteFragment status should be ok", returnValue.getReturnCode() == BKPConstants.SF_OK);
        }
        proxyClient.writeCloseExtent(extentId);

        for (int i = 1; i <= numOfFragments; i++) {
            ReadFragmentReturnValue readFragmentReturnValue = proxyClient.readFragment(extentId, i, 1024);
            Assert.assertTrue("ReadFragment status should be ok",
                    readFragmentReturnValue.getReturnCode() == BKPConstants.SF_OK);
            Assert.assertTrue("read FragmentData should be - fragment" + i,
                    new String(readFragmentReturnValue.getFragmentData()).equals("fragment" + i));
        }

        proxyClient.close();
    }

    @Test(timeout = 60000)
    public void fragmentsAsyncWriteReadTest() throws IOException, InterruptedException {
        addAndStartBKP(5555);
        BKProxyClient proxyClient = new BKProxyClient("localhost", 5555);
        proxyClient.init();
        CreateExtentReturnValue createExtentReturnValue = proxyClient.createExtent();
        Assert.assertTrue("Extent should be created successfully",
                createExtentReturnValue.getReturnCode() == BKPConstants.SF_OK);
        byte[] extentId = createExtentReturnValue.getExtentId();
        int numOfFragments = 10;
        for (int i = 1; i <= numOfFragments; i++) {
            ReturnValue returnValue = proxyClient.asyncWriteFragment(extentId, i, ("fragment" + i).getBytes());
            Assert.assertTrue("WriteFragment status should be ok", returnValue.getReturnCode() == BKPConstants.SF_OK);
        }

        Thread.sleep(1000);

        for (int i = 1; i <= numOfFragments; i++) {
            AsyncWriteStatusReturnValue asyncWriteStatusReturnValue = proxyClient.asyncWriteStatus(extentId, i, 1000);
            Assert.assertTrue("AsyncWriteStatus status should be ok",
                    asyncWriteStatusReturnValue.getReturnCode() == BKPConstants.SF_OK);
            Assert.assertTrue("AsyncWriteStatus status should be ok",
                    asyncWriteStatusReturnValue.getAsyncWriteStatus() == BKPConstants.SF_OK);
        }

        proxyClient.writeCloseExtent(extentId);
        proxyClient.close();
    }

    @Test(timeout = 60000)
    public void extentDeleteTest() throws IOException, InterruptedException {
        addAndStartBKP(5555);
        BKProxyClient proxyClient = new BKProxyClient("localhost", 5555);
        proxyClient.init();

        CreateExtentReturnValue createExtentReturnValue = proxyClient.createExtent();
        byte[] extentId = createExtentReturnValue.getExtentId();
        Assert.assertTrue("ExtentIds List should be of size 1",
                proxyClient.getExtentsList().getExtentIdsList().size() == 1);
        ReturnValue returnValue = proxyClient.deleteExtent(extentId);
        Assert.assertTrue("Returnvalue of ExtentDelete should be OK",
                returnValue.getReturnCode() == BKPConstants.SF_OK);

        proxyClient.createExtent();
        proxyClient.createExtent();
        Assert.assertTrue("ExtentIds List should be of size 2",
                proxyClient.getExtentsList().getExtentIdsList().size() == 2);
        returnValue = proxyClient.deleteAllExtents();
        Assert.assertTrue("Returnvalue of DeleteAllExtents should be OK",
                returnValue.getReturnCode() == BKPConstants.SF_OK);

        Assert.assertTrue("ExtentIds List should be of size 2",
                proxyClient.getExtentsList().getExtentIdsList().size() == 0);
        proxyClient.close();
    }

    static final class PrimitiveByteArrayWrapper {
        private final byte[] data;

        PrimitiveByteArrayWrapper(byte[] data) {
            if (data == null) {
                throw new NullPointerException();
            }
            this.data = data;
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof PrimitiveByteArrayWrapper)) {
                return false;
            }
            return Arrays.equals(data, ((PrimitiveByteArrayWrapper) other).data);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(data);
        }
    }

}
