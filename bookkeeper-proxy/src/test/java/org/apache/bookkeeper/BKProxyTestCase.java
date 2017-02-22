package org.apache.bookkeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.conf.BookKeeperProxyConfiguration;
import org.apache.bookkeeper.meta.LongHierarchicalLedgerManagerFactory;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.util.LedgerIdFormatter.UUIDLedgerIdFormatter;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

public class BKProxyTestCase extends BookKeeperClusterTestCase {

    public static final String SPLITREGEX = "-";
    public static final String NEWLINE = "\n";
    public static final String BKPDETAILS = "BKProxyDetails";
    public static final String NUMOFTHREADS = "NumOfThreads";
    public static final String THREADDETAILS = "ThreadDetails";
    public static final String NUMOFSLOTS = "NumOfSlots";
    public static final String VERSION = "Version";
    public static final String BKPOPERATION = "BKPOperation";
    public static final String PREOPSLEEP = "PreOpSleep";
    public static final int NUMOFSECSTOWAITFORCOMPLETION = 30;
    private  List<Throwable> currentTestScenarioExceptions;

    public BKProxyTestCase() {
        super(3);
        baseConf.setLedgerManagerFactoryClass(LongHierarchicalLedgerManagerFactory.class);
        baseClientConf.setLedgerManagerFactoryClass(LongHierarchicalLedgerManagerFactory.class);
    }

    @Rule
    public TestWatcher testWatcher = new TestWatcher() {
        @Override
        protected void starting(final Description description) {
            String methodName = description.getMethodName();
            String className = description.getClassName();
            className = className.substring(className.lastIndexOf('.') + 1);
            System.out.println("-------------Starting JUnit-test: " + className + " " + methodName + "-------------");
        }

        @Override
        protected void finished(final Description description) {
            String methodName = description.getMethodName();
            String className = description.getClassName();
            className = className.substring(className.lastIndexOf('.') + 1);
            System.out.println("-------------Finished JUnit-test: " + className + " " + methodName + "-------------");
        }

        @Override
        protected void succeeded(final Description description) {
            String methodName = description.getMethodName();
            String className = description.getClassName();
            className = className.substring(className.lastIndexOf('.') + 1);
            System.out.println("-------------Succeeded JUnit-test: " + className + " " + methodName + "-------------");
        }

        @Override
        protected void failed(Throwable e, final Description description) {
            String methodName = description.getMethodName();
            String className = description.getClassName();
            className = className.substring(className.lastIndexOf('.') + 1);
            System.out.println("-------------");
            System.out.println("Failed JUnit-test: " + className + " " + methodName
                    + " Following are the stacktraces for the Exceptions -");
            for (Throwable t : currentTestScenarioExceptions) {
                t.printStackTrace();
            }
            System.out.println("-------------");
        }
    };

    private Thread.UncaughtExceptionHandler threadExceptionHandler = new Thread.UncaughtExceptionHandler() {
        @Override
        public void uncaughtException(Thread t, Throwable e) {
            BKPClientThread bkpClientThread = (BKPClientThread) t;
            Throwable throwable = new Throwable(String.format(
                    "Operation at Timeslot: %d in ThreadId: %s has failed because of unexpected Exception/Error: %s",
                    TestScenarioState.getCurrentTestScenarioState().getCurrentTimeSlot(), bkpClientThread.getThreadId(),
                    e), e);
            bkpClientThread.setThreadException(throwable);
            TestScenarioState.getCurrentTestScenarioState().getCurrentTestScenarioThreadCountDownLatch().countDown();
        }
    };

    @Before
    public void testcaseSetup() throws InterruptedException {
        BKPClientThread.timeoutDurationInSecs = 4;
        TestScenarioState.instantiateCurrentTestScenarioState();
        TestScenarioState currentScenario = TestScenarioState.getCurrentTestScenarioState();
        currentTestScenarioExceptions = new ArrayList<Throwable>();
        currentScenario.getCommonBKPConfig().setZkServers(zkUtil.getZooKeeperConnectString());
        currentScenario.getCommonBKPConfig().setLedgerIdFormatterClass(UUIDLedgerIdFormatter.class);
    }

    @After
    public void testcaseCleanup() {
        TestScenarioState currentScenario = TestScenarioState.getCurrentTestScenarioState();
        currentScenario.closeAllClientSocketChannels();
        currentScenario.shutDownAllBkProxies();
        TestScenarioState.clearCurrentTestScenarioState();
    }

    /**
     * Causes the zk server to sleep for the specified amount of time.
     * During this time interval zk clients can't communicate with the server.
     */
    public void pauseZkServers(int sleepSecs) throws  InterruptedException, IOException {
        CountDownLatch latch = new CountDownLatch(1);
        zkUtil.sleepServer(sleepSecs, latch);
        latch.await();
    }

    /**
     * In this testcase Ledger is created, opened and closed for write and read. It just tests the basic functionality of create, open and close
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void basicTestWithLedgerCreateAndClose() throws IOException, InterruptedException {
        String testDefinition =         BKPDETAILS + "-BKP1-5555\n"
                                    +   NUMOFTHREADS + "-1\n"
                                    +   THREADDETAILS + "-Thread1-BKP1\n"
                                    +   NUMOFSLOTS + "-5\n"
                                    +   BKPOPERATION + "-0-Thread1-"+BKPConstants.LedgerCreateReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-1-Thread1-"+BKPConstants.LedgerWriteCloseReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    // trying to create a new Extent/Ledger with the same id should fail
                                    +   BKPOPERATION + "-2-Thread1-"+BKPConstants.LedgerCreateReq+"-ext1-"+BKPConstants.SF_ErrorExist+"\n"
                                    +   BKPOPERATION + "-3-Thread1-"+BKPConstants.LedgerOpenReadReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-4-Thread1-"+BKPConstants.LedgerReadCloseReq+"-ext1-"+BKPConstants.SF_OK+"\n";
        executeTestcase(testDefinition);
    }

    /**
     * In this testcase various operations are tried with negative ledgerid
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void negativeLedgerIdTest() throws IOException, InterruptedException {
        String testDefinition =         BKPDETAILS + "-BKP1-5555\n"
                                    +   NUMOFTHREADS + "-3\n"
                                    +   THREADDETAILS + "-Thread1-BKP1\n"
                                    +   THREADDETAILS + "-Thread2-BKP1\n"
                                    +   THREADDETAILS + "-Thread3-BKP1\n"
                                    +   NUMOFSLOTS + "-5\n"
                                    +   BKPOPERATION + "-0-Thread1-"+BKPConstants.LedgerCreateReq+"-"+TestScenarioState.NEGATIVEEXTENTID+"-"+BKPConstants.SF_ErrorIncorrectParameter+"\n"
                                    +   BKPOPERATION + "-1-Thread2-"+BKPConstants.LedgerWriteCloseReq+"-"+TestScenarioState.NEGATIVEEXTENTID+"-"+BKPConstants.SF_ErrorIncorrectParameter+"\n"
                                    +   BKPOPERATION + "-3-Thread3-"+BKPConstants.LedgerOpenReadReq+"-"+TestScenarioState.NEGATIVEEXTENTID+"-"+BKPConstants.SF_ErrorIncorrectParameter+"\n";
        executeTestcase(testDefinition);
    }

    /**
     * In this testcase four letters commands are tested.
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void fourLettersCommandTest() throws IOException, InterruptedException {
        String testDefinition =         BKPDETAILS + "-BKP1-5555\n"
                                    +   NUMOFTHREADS + "-6\n"
                                    +   THREADDETAILS + "-Thread1-BKP1\n"
                                    +   THREADDETAILS + "-Thread2-BKP1\n"
                                    +   THREADDETAILS + "-Thread3-BKP1\n"
                                    +   THREADDETAILS + "-Thread4-BKP1\n"
                                    +   THREADDETAILS + "-Thread5-BKP1\n"
                                    +   THREADDETAILS + "-Thread6-BKP1\n"
                                    +   NUMOFSLOTS + "-2\n"
                                    +   BKPOPERATION + "-0-Thread1-"+Operation.FourLettersReq+"-"+CommandExecutor.RUOKCMD+"-"+CommandExecutor.IMOK_STRING+"\n"
                                    +   BKPOPERATION + "-0-Thread2-"+Operation.FourLettersReq+"-"+CommandExecutor.RUOKCMD+"-"+CommandExecutor.IMOK_STRING+"\n"
                                    +   BKPOPERATION + "-1-Thread3-"+Operation.FourLettersReq+"-"+CommandExecutor.PVERCMD+"-"+BKPConstants.SFS_CURRENT_VERSION+"\n"
                                    +   BKPOPERATION + "-1-Thread4-"+Operation.FourLettersReq+"-"+CommandExecutor.ZKOKCMD+"-"+CommandExecutor.ZK_CONNECTED_STRING+"\n"
                                    +   BKPOPERATION + "-1-Thread5-"+Operation.FourLettersReq+"-"+CommandExecutor.BKOKCMD+"-"+CommandExecutor.ALLBOOKIESOK_STRING+"\n"
                                    +   BKPOPERATION + "-1-Thread6-"+Operation.FourLettersReq+"-"+CommandExecutor.BKOKCMD+"-"+CommandExecutor.ALLBOOKIESOK_STRING+"\n";
        executeTestcase(testDefinition);
    }

    /**
     * In this testcase we use lower and higher rev protocol versions and make sure that the proxy server is backward compatible and rejects higher
     * level requests properly.
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void basicVersionCompatibilityTest() throws IOException, InterruptedException {
        String testDefinition =         BKPDETAILS + "-BKP1-5555\n"
                                    +   NUMOFTHREADS + "-2\n"
                                    +   THREADDETAILS + "-Thread1-BKP1\n"
                                    +   THREADDETAILS + "-Thread2-BKP1\n"
                                    +   NUMOFSLOTS + "-3\n"
                                    // lower rev version should not be accepted
                                    +   VERSION + "-" + (BKPConstants.SFS_CURRENT_VERSION - 1) + "\n"
                                    +   BKPOPERATION + "-0-Thread1-"+BKPConstants.LedgerCreateReq+"-ext1-"+BKPConstants.SF_ErrorUnknownVersion+"\n"
                                    // current version should be accepted
                                    +   VERSION + "-" + BKPConstants.SFS_CURRENT_VERSION + "\n"
                                    +   BKPOPERATION + "-1-Thread2-"+BKPConstants.LedgerCreateReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    // higher version should be rejected
                                    +   VERSION + "-" + (BKPConstants.SFS_CURRENT_VERSION + 1) + "\n"
                                    +   BKPOPERATION + "-2-Thread2-"+BKPConstants.LedgerCreateReq+"-ext1-"+BKPConstants.SF_ErrorUnknownVersion+"\n";
        executeTestcase(testDefinition);
    }

    /**
     * In this testcase Ledger is created, opened and closed for write and read.
     * Then LedgerDeleteAll is called. It is called twice to make sure it is ok
     * to call even though it is already deleted all.
     *
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void basicLedgerDeleteAllTest() throws IOException, InterruptedException {

        String testDefinition =         BKPDETAILS + "-BKP1-5555\n"
                                    +   NUMOFTHREADS + "-1\n"
                                    +   THREADDETAILS + "-Thread1-BKP1\n"
                                    +   NUMOFSLOTS + "-7\n"
                                    +   BKPOPERATION + "-0-Thread1-"+BKPConstants.LedgerCreateReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-1-Thread1-"+BKPConstants.LedgerWriteCloseReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-2-Thread1-"+BKPConstants.LedgerOpenReadReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-3-Thread1-"+BKPConstants.LedgerReadCloseReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-4-Thread1-"+BKPConstants.LedgerDeleteAllReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-5-Thread1-"+BKPConstants.LedgerListGetReq+"-ext1-"+BKPConstants.SF_OK+"-ext1-\n"
                                    +   BKPOPERATION + "-6-Thread1-"+BKPConstants.LedgerDeleteAllReq+"-ext1-"+BKPConstants.SF_OK+"\n";
        executeTestcase(testDefinition);
    }

    /**
     * In this testcase LedgerList is called at several places - before and
     * after Ledger Write and Read, Open and close.
     *
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void basicLedgerListGetReqTest() throws IOException, InterruptedException {
        String testDefinition =         BKPDETAILS + "-BKP1-5555\n"
                                    +   NUMOFTHREADS + "-1\n"
                                    +   THREADDETAILS + "-Thread1-BKP1\n"
                                    +   NUMOFSLOTS + "-7\n"
                                    +   BKPOPERATION + "-0-Thread1-"+BKPConstants.LedgerCreateReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-1-Thread1-"+BKPConstants.LedgerListGetReq+"-ext1-"+BKPConstants.SF_OK+"-ext1-\n"
                                    +   BKPOPERATION + "-2-Thread1-"+BKPConstants.LedgerWriteCloseReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-3-Thread1-"+BKPConstants.LedgerListGetReq+"-ext1-"+BKPConstants.SF_OK+"-ext1-\n"
                                    +   BKPOPERATION + "-4-Thread1-"+BKPConstants.LedgerOpenReadReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-5-Thread1-"+BKPConstants.LedgerReadCloseReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-6-Thread1-"+BKPConstants.LedgerListGetReq+"-ext1-"+BKPConstants.SF_OK+"-ext1-\n";
        executeTestcase(testDefinition);
    }

    /**
     * In this test case we create 201 ledgers and close them(so that ledgerMetadata isn't cached).
     * Then we retrieve the list using creatorId via LedgerListGetReq. It sends the response in batches of 100.
     * We should get the response in 3 batches: the first 2 containing 100 and the last batch with 1.
     *
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void multiBatchLedgerListGetRespTest() throws IOException, InterruptedException {
        StringBuilder sb1 = new StringBuilder();
        StringBuilder sb2 = new StringBuilder();
        int slot = 0;
        int numLedgers = 2*BKPConstants.LEDGER_LIST_BATCH_SIZE + 1;
        for (int i = 0; i < numLedgers; i++) {
            sb1.append(BKPOPERATION).append("-").append(slot++).append("-Thread1-").append(BKPConstants.LedgerCreateReq)
                    .append("-ext").append(i).append("-").append(BKPConstants.SF_OK).append("\n");
            sb1.append(BKPOPERATION).append("-").append(slot++).append("-Thread1-")
                    .append(BKPConstants.LedgerWriteCloseReq).append("-ext").append(i).append("-")
                    .append(BKPConstants.SF_OK).append("\n");
            if (i == 0) {
                sb2.append("-ext").append(i);
            } else {
                sb2.append(":ext").append(i);
            }
        }
        sb2.append("-\n");
        String opsStrings = sb1.toString();
        String expectedSet = sb2.toString();

        int numSlots = slot + 1;
        String testDefinition =         BKPDETAILS + "-BKP1-5555\n"
                                    +   NUMOFTHREADS + "-1\n"
                                    +   THREADDETAILS + "-Thread1-BKP1\n"
                                    +   NUMOFSLOTS + "-" + numSlots + "\n"
                                    +   opsStrings
                                    +   BKPOPERATION + "-" + slot + "-Thread1-"+BKPConstants.LedgerListGetReq+"-ext1-"+BKPConstants.SF_OK+ expectedSet;
        executeTestcase(testDefinition);
    }

    /**
     * In this testcase it is tried to writeclose, readclose and openread for non-existing extents.
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void basicTestWithLedgerCreateAndNonExistingClose() throws IOException, InterruptedException {
        String testDefinition =         BKPDETAILS + "-BKP1-5555\n"
                                    +   NUMOFTHREADS + "-1\n"
                                    +   THREADDETAILS + "-Thread1-BKP1\n"
                                    +   NUMOFSLOTS + "-7\n"
                                    +   BKPOPERATION + "-0-Thread1-"+BKPConstants.LedgerCreateReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-1-Thread1-"+BKPConstants.LedgerWriteCloseReq+"-extn-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-2-Thread1-"+BKPConstants.LedgerWriteCloseReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-3-Thread1-"+BKPConstants.LedgerOpenReadReq+"-extn-"+BKPConstants.SF_ErrorNoSuchExtent+"\n"
                                    +   BKPOPERATION + "-4-Thread1-"+BKPConstants.LedgerOpenReadReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-5-Thread1-"+BKPConstants.LedgerReadCloseReq+"-extn-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-6-Thread1-"+BKPConstants.LedgerReadCloseReq+"-extn-"+BKPConstants.SF_OK+"\n";
        executeTestcase(testDefinition);
    }

    /**
     * In this testcase, we do simple write and read fragment operations. With no concurrency anywhere.
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void simpleWriteAndReadLedger() throws IOException, InterruptedException {
        String testDefinition =         BKPDETAILS + "-BKP1-5555\n"
                                    +   NUMOFTHREADS + "-3\n"
                                    +   THREADDETAILS + "-Thread1-BKP1\n"
                                    +   THREADDETAILS + "-Thread2-BKP1\n"
                                    +   THREADDETAILS + "-Thread3-BKP1\n"
                                    +   NUMOFSLOTS + "-11\n"
                                    +   BKPOPERATION + "-0-Thread1-"+BKPConstants.LedgerCreateReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-1-Thread1-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-1-1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-2-Thread2-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-2-20-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-3-Thread3-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-3-30000-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-4-Thread1-"+BKPConstants.LedgerWriteCloseReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-5-Thread1-"+BKPConstants.LedgerOpenReadReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-6-Thread1-"+BKPConstants.LedgerReadEntryReq+"-ext1-1-1000-"+BKPConstants.SF_OK+"-1\n"
                                    +   BKPOPERATION + "-7-Thread2-"+BKPConstants.LedgerReadEntryReq+"-ext1-2-1000-"+BKPConstants.SF_OK+"-20\n"
                                    // this operation would fail because the actual entry size is 30000 but we are expecting only 1000
                                    +   BKPOPERATION + "-8-Thread3-"+BKPConstants.LedgerReadEntryReq+"-ext1-3-1000-"+BKPConstants.SF_ShortRead+"-0\n"
                                    +   BKPOPERATION + "-9-Thread3-"+BKPConstants.LedgerReadEntryReq+"-ext1-3-400000000-"+BKPConstants.SF_OK+"-30000\n"
                                    +   BKPOPERATION + "-10-Thread1-"+BKPConstants.LedgerReadCloseReq+"-ext1-"+BKPConstants.SF_OK+"\n";
        executeTestcase(testDefinition);
    }

    /** In this testcase, we do simple write and read fragment operations.
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void simpleAsyncWriteAndReadLedger() throws IOException, InterruptedException {
        String testDefinition =         BKPDETAILS + "-BKP1-5555\n"
                                    +   NUMOFTHREADS + "-3\n"
                                    +   THREADDETAILS + "-Thread1-BKP1\n"
                                    +   THREADDETAILS + "-Thread2-BKP1\n"
                                    +   THREADDETAILS + "-Thread3-BKP1\n"
                                    +   NUMOFSLOTS + "-13\n"
                                    +   BKPOPERATION + "-0-Thread1-"+BKPConstants.LedgerCreateReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-1-Thread1-"+BKPConstants.LedgerAsyncWriteEntryReq+"-ext1-1-1-"+BKPConstants.SF_OK+"\n"
                                    // For the following Async Write Status we are doing preopsleep for 1 sec, so we should get OK status but not InProgress status,
                                    // though we are awaiting for 0 msecs time for the status response
                                    +   PREOPSLEEP + "-1000\n"
                                    +   BKPOPERATION + "-2-Thread1-"+BKPConstants.LedgerAsyncWriteStatusReq+"-ext1-1-0-"+BKPConstants.SF_OK+"-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-3-Thread2-"+BKPConstants.LedgerAsyncWriteEntryReq+"-ext1-2-20-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-3-Thread3-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-3-300-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-4-Thread3-"+BKPConstants.LedgerAsyncWriteEntryReq+"-ext1-4-30000-"+BKPConstants.SF_OK+"\n"
                                    // Here for the following statusreqs we are having sufficient await time, so we should get OK status
                                    +   BKPOPERATION + "-5-Thread3-"+BKPConstants.LedgerAsyncWriteStatusReq+"-ext1-2-0-"+BKPConstants.SF_OK+"-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-5-Thread2-"+BKPConstants.LedgerAsyncWriteStatusReq+"-ext1-4-1000-"+BKPConstants.SF_OK+"-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-6-Thread1-"+BKPConstants.LedgerWriteCloseReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-7-Thread1-"+BKPConstants.LedgerOpenReadReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-8-Thread1-"+BKPConstants.LedgerReadEntryReq+"-ext1-1-1000-"+BKPConstants.SF_OK+"-1\n"
                                    +   BKPOPERATION + "-9-Thread2-"+BKPConstants.LedgerReadEntryReq+"-ext1-2-1000-"+BKPConstants.SF_OK+"-20\n"
                                    // this operation would fail because the actual entry size is 30000 but we are expecting only 1000
                                    +   BKPOPERATION + "-10-Thread3-"+BKPConstants.LedgerReadEntryReq+"-ext1-4-1000-"+BKPConstants.SF_ShortRead+"-0\n"
                                    +   BKPOPERATION + "-11-Thread3-"+BKPConstants.LedgerReadEntryReq+"-ext1-4-400000000-"+BKPConstants.SF_OK+"-30000\n"
                                    +   BKPOPERATION + "-11-Thread2-"+BKPConstants.LedgerReadEntryReq+"-ext1-3-1000-"+BKPConstants.SF_OK+"-300\n"
                                    +   BKPOPERATION + "-12-Thread1-"+BKPConstants.LedgerReadCloseReq+"-ext1-"+BKPConstants.SF_OK+"\n";
        executeTestcase(testDefinition);
    }

    /**
     * In this testcase, we do advanced AsyncWrite/Status operations - status requests for InProgress requests, infinite wait for status,
     * bad writestatus requests
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void asyncWriteStatusWithAdvancedScenarios() throws IOException, InterruptedException {
        String testDefinition =         BKPDETAILS + "-BKP1-5555\n"
                                    +   NUMOFTHREADS + "-2\n"
                                    +   THREADDETAILS + "-Thread1-BKP1\n"
                                    +   THREADDETAILS + "-Thread2-BKP1\n"
                                    +   NUMOFSLOTS + "-9\n"
                                    +   BKPOPERATION + "-0-Thread1-"+BKPConstants.LedgerCreateReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-1-Thread1-"+BKPConstants.LedgerAsyncWriteEntryReq+"-ext1-1-1000000-"+BKPConstants.SF_OK+"\n"
                                    // in the following operation we are waiting for quite sometime to get the result, so it should succeed
                                    +   BKPOPERATION + "-3-Thread2-"+BKPConstants.LedgerAsyncWriteStatusReq+"-ext1-1-3000-"+BKPConstants.SF_OK+"-"+BKPConstants.SF_OK+"\n"
                                    // here we are doing out of order async write operation
                                    +   BKPOPERATION + "-3-Thread1-"+BKPConstants.LedgerAsyncWriteEntryReq+"-ext1-3-20-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-4-Thread1-"+BKPConstants.LedgerAsyncWriteStatusReq+"-ext1-3-1-"+BKPConstants.SF_OK+"-"+BKPConstants.SF_StatusInProgress+"\n"

                                    // in this slot, in Thread2 we are writing fragment 2 after 5000 msecs preop sleep. But in thread1 we are waiting forever to get status of
                                    // the asyncwrite of fragment 3. Since we are waiting for status forever in Thread1 we will eventually get ok status response.
                                    +   PREOPSLEEP + "-5000\n"
                                    +   BKPOPERATION + "-5-Thread2-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-2-20-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-5-Thread1-"+BKPConstants.LedgerAsyncWriteStatusReq+"-ext1-3-"+ LedgerAsyncWriteStatusReqBKPOperation.INFINITE_TIMEOUT+"-"+BKPConstants.SF_OK+"-"+BKPConstants.SF_OK+"\n"
                                    // following are BadRequest scenarios according to the contract of AsyncAPI
                                    +   BKPOPERATION + "-6-Thread1-"+BKPConstants.LedgerAsyncWriteStatusReq+"-ext1-3-0-"+BKPConstants.SF_ErrorIllegalOperation+"\n"
                                    +   BKPOPERATION + "-7-Thread1-"+BKPConstants.LedgerAsyncWriteStatusReq+"-ext1-2-0-"+BKPConstants.SF_ErrorIllegalOperation+"\n"
                                    +   BKPOPERATION + "-7-Thread2-"+BKPConstants.LedgerAsyncWriteStatusReq+"-ext2-2-0-"+BKPConstants.SF_ErrorIllegalOperation+"\n"
                                    +   BKPOPERATION + "-8-Thread2-"+BKPConstants.LedgerWriteCloseReq+"-ext1-"+BKPConstants.SF_OK+"\n";
        executeTestcase(testDefinition);
    }

    /**
     * In this testcase, we try to write fragment after it is WriteClosed.It is expected to return ErrorStatus.
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void writeLedgerAfterWriteClose() throws IOException, InterruptedException {
        String testDefinition =         BKPDETAILS + "-BKP1-5555\n"
                                    +   NUMOFTHREADS + "-3\n"
                                    +   THREADDETAILS + "-Thread1-BKP1\n"
                                    +   THREADDETAILS + "-Thread2-BKP1\n"
                                    +   THREADDETAILS + "-Thread3-BKP1\n"
                                    +   NUMOFSLOTS + "-6\n"
                                    +   BKPOPERATION + "-0-Thread1-"+BKPConstants.LedgerCreateReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-1-Thread1-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-1-1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-2-Thread2-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-2-20-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-3-Thread3-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-3-30000-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-4-Thread1-"+BKPConstants.LedgerWriteCloseReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-5-Thread3-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-4-30000-"+BKPConstants.SF_ErrorIllegalOperation+"\n";
        executeTestcase(testDefinition);
    }

    /**
     * In this testcase, we try to write fragment after writing Fragment0. It is expected to return ErrorStatus.
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void writeLedgerAfterFragment0() throws IOException, InterruptedException {
        String testDefinition =         BKPDETAILS + "-BKP1-5555\n"
                                    +   NUMOFTHREADS + "-3\n"
                                    +   THREADDETAILS + "-Thread1-BKP1\n"
                                    +   THREADDETAILS + "-Thread2-BKP1\n"
                                    +   THREADDETAILS + "-Thread3-BKP1\n"
                                    +   NUMOFSLOTS + "-6\n"
                                    +   BKPOPERATION + "-0-Thread1-"+BKPConstants.LedgerCreateReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-1-Thread1-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-1-1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-2-Thread2-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-2-20-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-3-Thread3-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-0-30000-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-5-Thread3-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-4-30000-"+BKPConstants.SF_ErrorIllegalOperation+"\n";
        executeTestcase(testDefinition);
    }

    /**
     * In this testcase we try to write extent of size greater than MAX_FRAG_SIZE and validate the error response
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void tryWritingOversizedFragment() throws IOException, InterruptedException {
        TestScenarioState currentScenario = TestScenarioState.getCurrentTestScenarioState();
        BookKeeperProxyConfiguration commonBKPConfig = currentScenario.getCommonBKPConfig();

        //Increasing TimeoutDuration because it might take longer time to create and send fragment of size BKPConstants.MAX_FRAG_SIZE
        BKPClientThread.timeoutDurationInSecs = 8;
        String testDefinition =         BKPDETAILS + "-BKP1-5555\n"
                                    +   NUMOFTHREADS + "-3\n"
                                    +   THREADDETAILS + "-Thread1-BKP1\n"
                                    +   THREADDETAILS + "-Thread2-BKP1\n"
                                    +   THREADDETAILS + "-Thread3-BKP1\n"
                                    +   NUMOFSLOTS + "-11\n"
                                    +   BKPOPERATION + "-0-Thread1-"+BKPConstants.LedgerCreateReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-1-Thread1-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-1-"+(commonBKPConfig.getMaxFragSize()+100)+"-"+BKPConstants.SF_ErrorIncorrectParameter+"\n"
                                    +   BKPOPERATION + "-2-Thread1-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-1-100-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-3-Thread2-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-2-20-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-4-Thread3-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-3-30000-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-5-Thread1-"+BKPConstants.LedgerWriteCloseReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-6-Thread1-"+BKPConstants.LedgerOpenReadReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-7-Thread1-"+BKPConstants.LedgerReadEntryReq+"-ext1-1-"+(commonBKPConfig.getMaxFragSize())+"-"+BKPConstants.SF_OK+"-100\n"
                                    +   BKPOPERATION + "-8-Thread2-"+BKPConstants.LedgerReadEntryReq+"-ext1-2-1000-"+BKPConstants.SF_OK+"-20\n"
                                    +   BKPOPERATION + "-9-Thread3-"+BKPConstants.LedgerReadEntryReq+"-ext1-3-400000000-"+BKPConstants.SF_OK+"-30000\n"
                                    +   BKPOPERATION + "-10-Thread1-"+BKPConstants.LedgerReadCloseReq+"-ext1-"+BKPConstants.SF_OK+"\n";
        executeTestcase(testDefinition);
    }

    /**
     * In this testcase Ledger is deleted and tried to write fragment to the deleted ledger.
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void ledgerDeleteTest() throws IOException, InterruptedException {
        String testDefinition =         BKPDETAILS + "-BKP1-5555\n"
                                    +   NUMOFTHREADS + "-3\n"
                                    +   THREADDETAILS + "-Thread1-BKP1\n"
                                    +   THREADDETAILS + "-Thread2-BKP1\n"
                                    +   THREADDETAILS + "-Thread3-BKP1\n"
                                    +   NUMOFSLOTS + "-7\n"
                                    +   BKPOPERATION + "-0-Thread1-"+BKPConstants.LedgerCreateReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-1-Thread3-"+BKPConstants.LedgerDeleteReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-2-Thread1-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-1-10-"+BKPConstants.SF_ErrorIllegalOperation+"\n"
                                    +   BKPOPERATION + "-3-Thread1-"+BKPConstants.LedgerWriteCloseReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-4-Thread2-"+BKPConstants.LedgerListGetReq+"-ext1-"+BKPConstants.SF_OK+"-:-\n"
                                    +   BKPOPERATION + "-5-Thread3-"+BKPConstants.LedgerDeleteReq+"-ext1-"+BKPConstants.SF_ErrorNoSuchExtent+"\n"
                                    +   BKPOPERATION + "-6-Thread2-"+BKPConstants.LedgerListGetReq+"-ext1-"+BKPConstants.SF_OK+"-:-\n";
        executeTestcase(testDefinition);
    }

    /**
     * In this testcase Ledger is deleted and tried to open the deleted ledeger for read
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void ledgerDeleteTestAfterWriteClose() throws IOException, InterruptedException {
        String testDefinition =         BKPDETAILS + "-BKP1-5555\n"
                                    +   NUMOFTHREADS + "-3\n"
                                    +   THREADDETAILS + "-Thread1-BKP1\n"
                                    +   THREADDETAILS + "-Thread2-BKP1\n"
                                    +   THREADDETAILS + "-Thread3-BKP1\n"
                                    +   NUMOFSLOTS + "-5\n"
                                    +   BKPOPERATION + "-0-Thread1-"+BKPConstants.LedgerCreateReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-1-Thread1-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-1-1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-2-Thread1-"+BKPConstants.LedgerWriteCloseReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-3-Thread3-"+BKPConstants.LedgerDeleteReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-4-Thread1-"+BKPConstants.LedgerOpenReadReq+"-ext1-"+BKPConstants.SF_ErrorNoSuchExtent+"\n";
        executeTestcase(testDefinition);
    }

    /**
     * In this testcase non-existing ledger is tried to delete
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void nonExistingledgerDeleteTest() throws IOException, InterruptedException {
        String testDefinition =         BKPDETAILS + "-BKP1-5555\n"
                                    +   NUMOFTHREADS + "-1\n"
                                    +   THREADDETAILS + "-Thread1-BKP1\n"
                                    +   NUMOFSLOTS + "-2\n"
                                    +   BKPOPERATION + "-0-Thread1-"+BKPConstants.LedgerDeleteReq+"-ext1-"+BKPConstants.SF_ErrorNoSuchExtent+"\n"
                                    +   BKPOPERATION + "-1-Thread1-"+BKPConstants.LedgerListGetReq+"-ext1-"+BKPConstants.SF_OK+"-:-\n";
        executeTestcase(testDefinition);
    }

    /**
     * In this testcase it is tried to write and read fragments from the non-existing ledgers. Also it is tried to read non-existing Fragment.
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void simpleWriteAndReadLedgerForNonExistingEntries() throws IOException, InterruptedException {
        String testDefinition =         BKPDETAILS + "-BKP1-5555\n"
                                    +   NUMOFTHREADS + "-3\n"
                                    +   THREADDETAILS + "-Thread1-BKP1\n"
                                    +   THREADDETAILS + "-Thread2-BKP1\n"
                                    +   THREADDETAILS + "-Thread3-BKP1\n"
                                    +   NUMOFSLOTS + "-11\n"
                                    +   BKPOPERATION + "-0-Thread1-"+BKPConstants.LedgerCreateReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-1-Thread1-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-1-10-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-2-Thread2-"+BKPConstants.LedgerWriteEntryReq+"-true-true-extn-2-10-"+BKPConstants.SF_ErrorIllegalOperation+"\n"
                                    +   BKPOPERATION + "-3-Thread3-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-2-10-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-4-Thread1-"+BKPConstants.LedgerWriteCloseReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-5-Thread1-"+BKPConstants.LedgerOpenReadReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-6-Thread1-"+BKPConstants.LedgerReadEntryReq+"-ext1-1-1000-"+BKPConstants.SF_OK+"-10\n"
                                    +   BKPOPERATION + "-7-Thread2-"+BKPConstants.LedgerReadEntryReq+"-extn-2-1000-"+BKPConstants.SF_ErrorNoSuchExtent+"\n"
                                    +   BKPOPERATION + "-8-Thread3-"+BKPConstants.LedgerReadEntryReq+"-ext1-9-1000-"+BKPConstants.SF_ErrorNoSuchFragmentClosed+"-10\n"
                                    +   BKPOPERATION + "-9-Thread1-"+BKPConstants.LedgerReadEntryReq+"-ext1-2-1000-"+BKPConstants.SF_OK+"-10\n"
                                    +   BKPOPERATION + "-10-Thread1-"+BKPConstants.LedgerReadCloseReq+"-ext1-"+BKPConstants.SF_OK+"\n";
        executeTestcase(testDefinition);
    }

    /**
     * In this testcase multiple fragments are written concurrently in a single timeslot.
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void concurrentWrites() throws IOException, InterruptedException {
        String testDefinition =         BKPDETAILS + "-BKP1-5555\n"
                                    +   NUMOFTHREADS + "-3\n"
                                    +   THREADDETAILS + "-Thread1-BKP1\n"
                                    +   THREADDETAILS + "-Thread2-BKP1\n"
                                    +   THREADDETAILS + "-Thread3-BKP1\n"
                                    +   NUMOFSLOTS + "-5\n"
                                    +   BKPOPERATION + "-0-Thread1-"+BKPConstants.LedgerCreateReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-1-Thread1-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-1-10-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-1-Thread2-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-2-10-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-1-Thread3-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-3-10-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-2-Thread2-"+BKPConstants.LedgerDeleteAllReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-3-Thread1-"+BKPConstants.LedgerWriteCloseReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-4-Thread3-"+BKPConstants.LedgerListGetReq+"-ext1-"+BKPConstants.SF_OK+"-:-\n";
        executeTestcase(testDefinition);
    }

    /**
     * In this testcase multiple fragments are read concurrently in a single
     * timeslot
     *
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void simpleWriteAndConcurrentReads() throws IOException, InterruptedException {
        String testDefinition =         BKPDETAILS + "-BKP1-5555\n"
                                    +   NUMOFTHREADS + "-3\n"
                                    +   THREADDETAILS + "-Thread1-BKP1\n"
                                    +   THREADDETAILS + "-Thread2-BKP1\n"
                                    +   THREADDETAILS + "-Thread3-BKP1\n"
                                    +   NUMOFSLOTS + "-8\n"
                                    +   BKPOPERATION + "-0-Thread1-"+BKPConstants.LedgerCreateReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-1-Thread1-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-1-10-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-2-Thread2-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-2-10-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-3-Thread3-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-3-10-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-4-Thread1-"+BKPConstants.LedgerWriteCloseReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-5-Thread1-"+BKPConstants.LedgerOpenReadReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-6-Thread1-"+BKPConstants.LedgerReadEntryReq+"-ext1-1-1000-"+BKPConstants.SF_OK+"-10\n"
                                    +   BKPOPERATION + "-6-Thread2-"+BKPConstants.LedgerReadEntryReq+"-ext1-2-1000-"+BKPConstants.SF_OK+"-10\n"
                                    +   BKPOPERATION + "-6-Thread3-"+BKPConstants.LedgerReadEntryReq+"-ext1-3-1000-"+BKPConstants.SF_OK+"-10\n"
                                    +   BKPOPERATION + "-7-Thread1-"+BKPConstants.LedgerReadCloseReq+"-ext1-"+BKPConstants.SF_OK+"\n";
        executeTestcase(testDefinition);
    }

    /**
     * In this testcase we test out-of-order writes.
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void outOfOrderWrites() throws IOException, InterruptedException {
        String testDefinition =         BKPDETAILS + "-BKP1-5555\n"
                                    +   NUMOFTHREADS + "-3\n"
                                    +   THREADDETAILS + "-Thread1-BKP1\n"
                                    +   THREADDETAILS + "-Thread2-BKP1\n"
                                    +   THREADDETAILS + "-Thread3-BKP1\n"
                                    +   NUMOFSLOTS + "-4\n"
                                    +   BKPOPERATION + "-0-Thread1-"+BKPConstants.LedgerCreateReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-1-Thread2-"+BKPConstants.LedgerWriteEntryReq+"-true-false-ext1-2-10-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-1-Thread3-"+BKPConstants.LedgerWriteEntryReq+"-true-false-ext1-3-10-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-2-Thread1-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-1-10-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-2-Thread2-"+BKPConstants.LedgerWriteEntryReq+"-false-true-ext1-2-10-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-2-Thread3-"+BKPConstants.LedgerWriteEntryReq+"-false-true-ext1-3-10-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-3-Thread1-"+BKPConstants.LedgerWriteCloseReq+"-ext1-"+BKPConstants.SF_OK+"\n";
        executeTestcase(testDefinition);
    }

    /**
     * In this testcase we write and read multiple ledgers concurrently
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void concurrentWriteAndReads() throws IOException, InterruptedException {
        String testDefinition =         BKPDETAILS + "-BKP1-5555\n"
                                    +   NUMOFTHREADS + "-6\n"
                                    +   THREADDETAILS + "-Thread1-BKP1\n"
                                    +   THREADDETAILS + "-Thread2-BKP1\n"
                                    +   THREADDETAILS + "-Thread3-BKP1\n"
                                    +   THREADDETAILS + "-Thread4-BKP1\n"
                                    +   THREADDETAILS + "-Thread5-BKP1\n"
                                    +   THREADDETAILS + "-Thread6-BKP1\n"
                                    +   NUMOFSLOTS + "-10\n"
                                    +   BKPOPERATION + "-0-Thread1-"+BKPConstants.LedgerCreateReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-1-Thread1-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-1-1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-2-Thread2-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-2-20-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-3-Thread3-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-3-30000-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-4-Thread1-"+BKPConstants.LedgerWriteCloseReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-4-Thread2-"+BKPConstants.LedgerListGetReq+"-ext1-"+BKPConstants.SF_OK+"-ext1-\n"
                                    +   BKPOPERATION + "-5-Thread4-"+BKPConstants.LedgerCreateReq+"-ext2-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-5-Thread1-"+BKPConstants.LedgerOpenReadReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-6-Thread4-"+BKPConstants.LedgerListGetReq+"-ext1-"+BKPConstants.SF_OK+"-ext1:ext2-\n"
                                    +   BKPOPERATION + "-6-Thread1-"+BKPConstants.LedgerReadEntryReq+"-ext1-1-1000-"+BKPConstants.SF_OK+"-1\n"
                                    +   BKPOPERATION + "-7-Thread2-"+BKPConstants.LedgerReadEntryReq+"-ext1-2-1000-"+BKPConstants.SF_OK+"-20\n"
                                    +   BKPOPERATION + "-8-Thread4-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext2-1-1000-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-8-Thread5-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext2-2-10000-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-8-Thread3-"+BKPConstants.LedgerReadEntryReq+"-ext1-3-400000000-"+BKPConstants.SF_OK+"-30000\n"
                                    +   BKPOPERATION + "-9-Thread4-"+BKPConstants.LedgerWriteCloseReq+"-ext2-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-9-Thread1-"+BKPConstants.LedgerReadCloseReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-9-Thread3-"+BKPConstants.LedgerListGetReq+"-ext1-"+BKPConstants.SF_OK+"-ext1:ext2-\n";
        executeTestcase(testDefinition);
    }

    /**
     * In this testcase we make LedgerStateReq at various phases (before first write, after couple of writes, after out-of-order write, before close).
     * Since we are doing all these operations before WriteClose, it would be using WriteHandle
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void testingLedgerLengthUsingWriteHandle() throws IOException, InterruptedException {
        String testDefinition =         BKPDETAILS + "-BKP1-5555\n"
                                    +   NUMOFTHREADS + "-3\n"
                                    +   THREADDETAILS + "-Thread1-BKP1\n"
                                    +   THREADDETAILS + "-Thread2-BKP1\n"
                                    +   THREADDETAILS + "-Thread3-BKP1\n"
                                    +   NUMOFSLOTS + "-17\n"
                                    +   BKPOPERATION + "-0-Thread1-"+BKPConstants.LedgerCreateReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-1-Thread3-"+BKPConstants.LedgerStatReq+"-ext1-"+BKPConstants.SF_OK+"-0\n"
                                    +   BKPOPERATION + "-2-Thread1-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-1-10-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-3-Thread3-"+BKPConstants.LedgerStatReq+"-ext1-"+BKPConstants.SF_OK+"-10\n"
                                    // here we are trying to get stat of non-existing extent, which should fail with ErrorNotFound error
                                    +   BKPOPERATION + "-3-Thread2-"+BKPConstants.LedgerStatReq+"-extn-"+BKPConstants.SF_ErrorNoSuchExtent+"-0\n"
                                    +   BKPOPERATION + "-4-Thread2-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-2-10-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-4-Thread3-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-3-10-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-5-Thread3-"+BKPConstants.LedgerStatReq+"-ext1-"+BKPConstants.SF_OK+"-30\n"
                                    +   BKPOPERATION + "-6-Thread2-"+BKPConstants.LedgerWriteEntryReq+"-true-false-ext1-5-10-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-6-Thread3-"+BKPConstants.LedgerWriteEntryReq+"-true-false-ext1-6-10-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-7-Thread1-"+BKPConstants.LedgerStatReq+"-ext1-"+BKPConstants.SF_OK+"-50\n"
                                    +   BKPOPERATION + "-8-Thread1-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-4-10-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-8-Thread2-"+BKPConstants.LedgerWriteEntryReq+"-false-true-ext1-5-10-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-8-Thread3-"+BKPConstants.LedgerWriteEntryReq+"-false-true-ext1-6-10-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-9-Thread3-"+BKPConstants.LedgerStatReq+"-ext1-"+BKPConstants.SF_OK+"-60\n"
                                    +   BKPOPERATION + "-10-Thread1-"+BKPConstants.LedgerWriteCloseReq+"-ext1-"+BKPConstants.SF_OK+"\n";
        executeTestcase(testDefinition);
    }

    /**
     * In this testcase we make LedgerStat request after writeclose, so ReadHandle will be used for StatReq
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void testingLedgerLengthUsingOpenedReadHandle() throws IOException, InterruptedException {
        String testDefinition =         BKPDETAILS + "-BKP1-5555\n"
                                    +   NUMOFTHREADS + "-3\n"
                                    +   THREADDETAILS + "-Thread1-BKP1\n"
                                    +   THREADDETAILS + "-Thread2-BKP1\n"
                                    +   THREADDETAILS + "-Thread3-BKP1\n"
                                    +   NUMOFSLOTS + "-8\n"
                                    +   BKPOPERATION + "-0-Thread1-"+BKPConstants.LedgerCreateReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-1-Thread1-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-1-10-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-2-Thread2-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-2-10-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-3-Thread3-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-3-10-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-4-Thread1-"+BKPConstants.LedgerWriteCloseReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-5-Thread1-"+BKPConstants.LedgerOpenReadReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-6-Thread1-"+BKPConstants.LedgerStatReq+"-ext1-"+BKPConstants.SF_OK+"-30\n"
                                    +   BKPOPERATION + "-7-Thread1-"+BKPConstants.LedgerReadCloseReq+"-ext1-"+BKPConstants.SF_OK+"\n";
        executeTestcase(testDefinition);
    }

    /**
     * In this testcase LedgerStatReq is made after both Write Handle and Read Handles are closed. So new ReadHandle will be used for stat req
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void testingLedgerLengthUsingReadHandle() throws IOException, InterruptedException {
        String testDefinition =         BKPDETAILS + "-BKP1-5555\n"
                                    +   NUMOFTHREADS + "-3\n"
                                    +   THREADDETAILS + "-Thread1-BKP1\n"
                                    +   THREADDETAILS + "-Thread2-BKP1\n"
                                    +   THREADDETAILS + "-Thread3-BKP1\n"
                                    +   NUMOFSLOTS + "-6\n"
                                    +   BKPOPERATION + "-0-Thread1-"+BKPConstants.LedgerCreateReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-1-Thread1-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-1-10-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-2-Thread2-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-2-10-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-3-Thread3-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-3-10-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-4-Thread1-"+BKPConstants.LedgerWriteCloseReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-5-Thread1-"+BKPConstants.LedgerStatReq+"-ext1-"+BKPConstants.SF_OK+"-30\n";
        executeTestcase(testDefinition);
    }

    /**
     * In this testcase multiple Ledgers are written concurrently
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void multipleLedgersWrites() throws IOException, InterruptedException {
        String testDefinition =         BKPDETAILS + "-BKP1-5555\n"
                                    +   NUMOFTHREADS + "-6\n"
                                    +   THREADDETAILS + "-Thread1-BKP1\n"
                                    +   THREADDETAILS + "-Thread2-BKP1\n"
                                    +   THREADDETAILS + "-Thread3-BKP1\n"
                                    +   THREADDETAILS + "-Thread4-BKP1\n"
                                    +   THREADDETAILS + "-Thread5-BKP1\n"
                                    +   THREADDETAILS + "-Thread6-BKP1\n"
                                    +   NUMOFSLOTS + "-5\n"
                                    +   BKPOPERATION + "-0-Thread1-"+BKPConstants.LedgerCreateReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-0-Thread4-"+BKPConstants.LedgerCreateReq+"-ext2-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-1-Thread1-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-1-100-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-1-Thread4-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext2-1-1000-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-2-Thread2-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-2-3000-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-2-Thread5-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext2-2-5000-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-3-Thread3-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-3-100-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-3-Thread6-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext2-3-1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-4-Thread1-"+BKPConstants.LedgerWriteCloseReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-4-Thread4-"+BKPConstants.LedgerWriteCloseReq+"-ext2-"+BKPConstants.SF_OK+"\n";
        executeTestcase(testDefinition);
    }

    /**
     * In this testcase multiple BookKeeper proxies are created and multiple ledgers are written concurrently
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void multipleExtentsWritesUsingMultipleBKProxies() throws IOException, InterruptedException {
        String testDefinition =         BKPDETAILS + "-BKP1-5555\n"
                                    +   BKPDETAILS + "-BKP2-7777\n"
                                    +   NUMOFTHREADS + "-6\n"
                                    +   THREADDETAILS + "-Thread1-BKP1\n"
                                    +   THREADDETAILS + "-Thread2-BKP1\n"
                                    +   THREADDETAILS + "-Thread3-BKP1\n"
                                    +   THREADDETAILS + "-Thread4-BKP2\n"
                                    +   THREADDETAILS + "-Thread5-BKP2\n"
                                    +   THREADDETAILS + "-Thread6-BKP2\n"
                                    +   NUMOFSLOTS + "-5\n"
                                    +   BKPOPERATION + "-0-Thread1-"+BKPConstants.LedgerCreateReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-0-Thread4-"+BKPConstants.LedgerCreateReq+"-ext2-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-1-Thread1-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-1-10-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-1-Thread4-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext2-1-10-"+BKPConstants.SF_OK+"\n"
                                    // trying to create a new Extent/Ledger with the already existing id (though it is created by other BKProxy instance) should fail
                                    +   BKPOPERATION + "-1-Thread5-"+BKPConstants.LedgerCreateReq+"-ext1-"+BKPConstants.SF_ErrorExist+"\n"
                                    +   BKPOPERATION + "-2-Thread2-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-2-10-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-2-Thread5-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext2-2-10-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-3-Thread3-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-3-10-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-3-Thread6-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext2-3-10-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-4-Thread1-"+BKPConstants.LedgerWriteCloseReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-4-Thread4-"+BKPConstants.LedgerWriteCloseReq+"-ext2-"+BKPConstants.SF_OK+"\n";
        executeTestcase(testDefinition);
    }

    /**
     * In this testcase multiple BookKeeper proxies are created and multiple ledgers are written and read concurrently
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void multipleExtentsWritesAndReadsUsingMultipleBKProxies() throws IOException, InterruptedException {
        String testDefinition =         BKPDETAILS + "-BKP1-5555\n"
                                    +   BKPDETAILS + "-BKP2-7777\n"
                                    +   NUMOFTHREADS + "-6\n"
                                    +   THREADDETAILS + "-Thread1-BKP1\n"
                                    +   THREADDETAILS + "-Thread2-BKP1\n"
                                    +   THREADDETAILS + "-Thread3-BKP1\n"
                                    +   THREADDETAILS + "-Thread4-BKP2\n"
                                    +   THREADDETAILS + "-Thread5-BKP2\n"
                                    +   THREADDETAILS + "-Thread6-BKP2\n"
                                    +   NUMOFSLOTS + "-10\n"
                                    +   BKPOPERATION + "-0-Thread1-"+BKPConstants.LedgerCreateReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-0-Thread4-"+BKPConstants.LedgerCreateReq+"-ext2-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-1-Thread1-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-1-10-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-1-Thread4-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext2-1-10-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-1-Thread2-"+BKPConstants.LedgerListGetReq+"-ext1-"+BKPConstants.SF_OK+"-ext1:ext2-\n"
                                    +   BKPOPERATION + "-1-Thread5-"+BKPConstants.LedgerListGetReq+"-ext1-"+BKPConstants.SF_OK+"-ext1:ext2-\n"
                                    +   BKPOPERATION + "-2-Thread2-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-2-10-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-2-Thread5-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext2-2-10-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-3-Thread3-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-3-10-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-3-Thread6-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext2-3-10-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-4-Thread1-"+BKPConstants.LedgerWriteCloseReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-4-Thread4-"+BKPConstants.LedgerWriteCloseReq+"-ext2-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-5-Thread1-"+BKPConstants.LedgerOpenReadReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-5-Thread4-"+BKPConstants.LedgerOpenReadReq+"-ext2-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-6-Thread1-"+BKPConstants.LedgerReadEntryReq+"-ext1-1-1000-"+BKPConstants.SF_OK+"-10\n"
                                    +   BKPOPERATION + "-6-Thread2-"+BKPConstants.LedgerReadEntryReq+"-ext1-2-1000-"+BKPConstants.SF_OK+"-10\n"
                                    +   BKPOPERATION + "-6-Thread3-"+BKPConstants.LedgerReadEntryReq+"-ext1-3-1000-"+BKPConstants.SF_OK+"-10\n"
                                    +   BKPOPERATION + "-6-Thread4-"+BKPConstants.LedgerReadEntryReq+"-ext2-1-1000-"+BKPConstants.SF_OK+"-10\n"
                                    +   BKPOPERATION + "-6-Thread5-"+BKPConstants.LedgerReadEntryReq+"-ext2-2-1000-"+BKPConstants.SF_OK+"-10\n"
                                    +   BKPOPERATION + "-6-Thread6-"+BKPConstants.LedgerReadEntryReq+"-ext2-3-1000-"+BKPConstants.SF_OK+"-10\n"
                                    +   BKPOPERATION + "-7-Thread1-"+BKPConstants.LedgerReadCloseReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-7-Thread4-"+BKPConstants.LedgerReadCloseReq+"-ext2-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-8-Thread1-"+BKPConstants.LedgerDeleteAllReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-9-Thread2-"+BKPConstants.LedgerListGetReq+"-ext1-"+BKPConstants.SF_OK+"-ext1:ext2-\n"
                                    +   BKPOPERATION + "-9-Thread4-"+BKPConstants.LedgerListGetReq+"-ext1-"+BKPConstants.SF_OK+"-ext1:ext2-\n";
        executeTestcase(testDefinition);
    }


    /**
     * Trying to read ledger fragments before write is closed
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void readLedgerBeforeWriteClose() throws IOException, InterruptedException {
        String testDefinition =         BKPDETAILS + "-BKP1-5555\n"
                                    +   NUMOFTHREADS + "-6\n"
                                    +   THREADDETAILS + "-Thread1-BKP1\n"
                                    +   THREADDETAILS + "-Thread2-BKP1\n"
                                    +   THREADDETAILS + "-Thread3-BKP1\n"
                                    +   THREADDETAILS + "-Thread4-BKP1\n"
                                    +   THREADDETAILS + "-Thread5-BKP1\n"
                                    +   THREADDETAILS + "-Thread6-BKP1\n"
                                    +   NUMOFSLOTS + "-7\n"
                                    +   BKPOPERATION + "-0-Thread1-"+BKPConstants.LedgerCreateReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-1-Thread1-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-1-10-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-2-Thread2-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-2-10-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-3-Thread3-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-3-10-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-4-Thread4-"+BKPConstants.LedgerOpenReadReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-5-Thread4-"+BKPConstants.LedgerReadEntryReq+"-ext1-1-1000-"+BKPConstants.SF_OK+"-10\n"
                                    +   BKPOPERATION + "-5-Thread5-"+BKPConstants.LedgerReadEntryReq+"-ext1-2-1000-"+BKPConstants.SF_OK+"-10\n"
                                    +   BKPOPERATION + "-5-Thread6-"+BKPConstants.LedgerReadEntryReq+"-ext1-3-1000-"+BKPConstants.SF_OK+"-10\n"
                                    +   BKPOPERATION + "-6-Thread4-"+BKPConstants.LedgerReadCloseReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-6-Thread1-"+BKPConstants.LedgerWriteCloseReq+"-ext1-"+BKPConstants.SF_OK+"\n";
        executeTestcase(testDefinition);
    }

    /**
     * In this testcase, we do simple write and read Fragment0 for reading last written Fragment.
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void simpleWriteAndReadFragment0() throws IOException, InterruptedException {
        String testDefinition =         BKPDETAILS + "-BKP1-5555\n"
                                    +   NUMOFTHREADS + "-3\n"
                                    +   THREADDETAILS + "-Thread1-BKP1\n"
                                    +   THREADDETAILS + "-Thread2-BKP1\n"
                                    +   THREADDETAILS + "-Thread3-BKP1\n"
                                    +   NUMOFSLOTS + "-10\n"
                                    +   BKPOPERATION + "-0-Thread1-"+BKPConstants.LedgerCreateReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-1-Thread1-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-1-1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-2-Thread2-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-2-20-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-3-Thread3-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-3-30000-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-4-Thread1-"+BKPConstants.LedgerWriteCloseReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-5-Thread1-"+BKPConstants.LedgerOpenReadReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-6-Thread1-"+BKPConstants.LedgerReadEntryReq+"-ext1-1-1000-"+BKPConstants.SF_OK+"-1\n"
                                    +   BKPOPERATION + "-7-Thread3-"+BKPConstants.LedgerReadEntryReq+"-ext1-3-400000000-"+BKPConstants.SF_OK+"-30000\n"
                                    +   BKPOPERATION + "-8-Thread3-"+BKPConstants.LedgerReadEntryReq+"-ext1-0-400000000-"+BKPConstants.SF_OK+"-30000\n"
                                    +   BKPOPERATION + "-9-Thread1-"+BKPConstants.LedgerReadCloseReq+"-ext1-"+BKPConstants.SF_OK+"\n";
        executeTestcase(testDefinition);
    }

    /**
     * In this testcase, we Write and read fragment 0
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void simpleWriteFragment0AndReadFragment0() throws IOException, InterruptedException {
        String testDefinition =         BKPDETAILS + "-BKP1-5555\n"
                                    +   NUMOFTHREADS + "-3\n"
                                    +   THREADDETAILS + "-Thread1-BKP1\n"
                                    +   THREADDETAILS + "-Thread2-BKP1\n"
                                    +   THREADDETAILS + "-Thread3-BKP1\n"
                                    +   NUMOFSLOTS + "-10\n"
                                    +   BKPOPERATION + "-0-Thread1-"+BKPConstants.LedgerCreateReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-1-Thread1-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-1-1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-2-Thread2-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-2-20-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-3-Thread3-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-0-30000-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-6-Thread1-"+BKPConstants.LedgerReadEntryReq+"-ext1-1-1000-"+BKPConstants.SF_OK+"-1\n"
                                    +   BKPOPERATION + "-7-Thread3-"+BKPConstants.LedgerReadEntryReq+"-ext1-2-400000000-"+BKPConstants.SF_OK+"-20\n"
                                    +   BKPOPERATION + "-8-Thread3-"+BKPConstants.LedgerReadEntryReq+"-ext1-0-400000000-"+BKPConstants.SF_OK+"-30000\n"
                                    +   BKPOPERATION + "-9-Thread1-"+BKPConstants.LedgerReadCloseReq+"-ext1-"+BKPConstants.SF_OK+"\n";
        executeTestcase(testDefinition);
    }

    /**
     * In this testcase, we try to read Fragment 0 without writeclosing, which should return SF_ErrorNoSuchFragment return status
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void readFragment0WithoutWriteClose() throws IOException, InterruptedException {
        String testDefinition =         BKPDETAILS + "-BKP1-5555\n"
                                    +   NUMOFTHREADS + "-3\n"
                                    +   THREADDETAILS + "-Thread1-BKP1\n"
                                    +   THREADDETAILS + "-Thread2-BKP1\n"
                                    +   THREADDETAILS + "-Thread3-BKP1\n"
                                    +   NUMOFSLOTS + "-10\n"
                                    +   BKPOPERATION + "-0-Thread1-"+BKPConstants.LedgerCreateReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-1-Thread1-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-1-1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-2-Thread2-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-2-20-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-3-Thread3-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-3-30000-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-4-Thread1-"+BKPConstants.LedgerOpenReadReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-5-Thread1-"+BKPConstants.LedgerReadEntryReq+"-ext1-1-1000-"+BKPConstants.SF_OK+"-1\n"
                                    +   BKPOPERATION + "-6-Thread3-"+BKPConstants.LedgerReadEntryReq+"-ext1-3-400000000-"+BKPConstants.SF_OK+"-30000\n"
                                    +   BKPOPERATION + "-7-Thread3-"+BKPConstants.LedgerReadEntryReq+"-ext1-0-400000000-"+BKPConstants.SF_ErrorNoSuchFragment+"-30000\n";
        executeTestcase(testDefinition);
    }

    /**
     * In this testcase LedgerOpenRecoverReq is called before LedgerWriteCloseReq and it should implicitly close the write
     * handle and fence the ledger for writes.
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void tryToOpenRecoveryBeforeWriteClose() throws IOException, InterruptedException {
        String testDefinition =         BKPDETAILS + "-BKP1-5555\n"
                                    +   NUMOFTHREADS + "-3\n"
                                    +   THREADDETAILS + "-Thread1-BKP1\n"
                                    +   THREADDETAILS + "-Thread2-BKP1\n"
                                    +   THREADDETAILS + "-Thread3-BKP1\n"
                                    +   NUMOFSLOTS + "-9\n"
                                    +   BKPOPERATION + "-0-Thread1-"+BKPConstants.LedgerCreateReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-1-Thread1-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-1-1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-2-Thread2-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-2-20-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-3-Thread3-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-3-30000-"+BKPConstants.SF_OK+"\n"
                                    // here we are trying to do OpenRecover in the same BKProxy and it is supposed to fail since write is not closed
                                    +   BKPOPERATION + "-4-Thread1-"+BKPConstants.LedgerOpenRecoverReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-5-Thread1-"+BKPConstants.LedgerWriteCloseReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    // by now write is closed, so open-recover should function normally
                                    +   BKPOPERATION + "-6-Thread1-"+BKPConstants.LedgerOpenRecoverReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-7-Thread1-"+BKPConstants.LedgerReadEntryReq+"-ext1-1-1000-"+BKPConstants.SF_OK+"-1\n"
                                    +   BKPOPERATION + "-7-Thread2-"+BKPConstants.LedgerReadEntryReq+"-ext1-2-1000-"+BKPConstants.SF_OK+"-20\n"
                                    // Reading past end of fenced ledger will return ErrorNotFoundClosed, rather than just ErrorNotFound
                                    +   BKPOPERATION + "-7-Thread3-"+BKPConstants.LedgerReadEntryReq+"-ext1-4-400000000-"+BKPConstants.SF_ErrorNoSuchFragmentClosed+"-30000\n"
                                    +   BKPOPERATION + "-8-Thread1-"+BKPConstants.LedgerReadCloseReq+"-ext1-"+BKPConstants.SF_OK+"\n";
        executeTestcase(testDefinition);
    }

    /**
     * In this testcase LedgerOpenRecoverReq is called when a NonRecoveryReadHandle is present. This should cause the
     * existing NonRecoveryRead handle to be closed a RecoveryRead handle to be opened.
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void tryToOpenRecoveryBeforeReadClose() throws IOException, InterruptedException {
        String testDefinition =         BKPDETAILS + "-BKP1-5555\n"
                                    +   NUMOFTHREADS + "-1\n"
                                    +   THREADDETAILS + "-Thread1-BKP1\n"
                                    +   NUMOFSLOTS + "-11\n"
                                    +   BKPOPERATION + "-0-Thread1-"+BKPConstants.LedgerCreateReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-1-Thread1-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-1-1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-2-Thread1-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-2-20-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-3-Thread1-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-3-30000-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-4-Thread1-"+BKPConstants.LedgerWriteCloseReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-5-Thread1-"+BKPConstants.LedgerOpenReadReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-6-Thread1-"+BKPConstants.LedgerReadEntryReq+"-ext1-1-1000-"+BKPConstants.SF_OK+"-1\n"
                                    +   BKPOPERATION + "-7-Thread1-"+BKPConstants.LedgerReadEntryReq+"-ext1-2-1000-"+BKPConstants.SF_OK+"-20\n"
                                    // Now we do an OpenRecoveryRead when there is a NonRecoveryRead handle present. This should work well.
                                    +   BKPOPERATION + "-8-Thread1-"+BKPConstants.LedgerOpenRecoverReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-9-Thread1-"+BKPConstants.LedgerReadEntryReq+"-ext1-3-30000-"+BKPConstants.SF_OK+"-30000\n"
                                    +   BKPOPERATION + "-10-Thread1-"+BKPConstants.LedgerReadCloseReq+"-ext1-"+BKPConstants.SF_OK+"\n";
        executeTestcase(testDefinition);
    }

    /**
     * In this testcase, after LedgerOpenRecoverReq we try to write a new fragment and it is expected to fail.
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void tryToWriteAfterOpenRecover() throws IOException, InterruptedException {
        String testDefinition =         BKPDETAILS + "-BKP1-5555\n"
                                    +   BKPDETAILS + "-BKP2-7777\n"
                                    +   NUMOFTHREADS + "-6\n"
                                    +   THREADDETAILS + "-Thread1-BKP1\n"
                                    +   THREADDETAILS + "-Thread2-BKP1\n"
                                    +   THREADDETAILS + "-Thread3-BKP1\n"
                                    +   THREADDETAILS + "-Thread4-BKP2\n"
                                    +   THREADDETAILS + "-Thread5-BKP2\n"
                                    +   THREADDETAILS + "-Thread6-BKP2\n"
                                    +   NUMOFSLOTS + "-8\n"
                                    +   BKPOPERATION + "-0-Thread1-"+BKPConstants.LedgerCreateReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-1-Thread1-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-1-1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-2-Thread2-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-2-20-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-3-Thread3-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-3-30000-"+BKPConstants.SF_OK+"\n"
                                    // here we are trying to OpenRecover in thread connected to other BKProxy (BKP2), so it should succeed
                                    +   BKPOPERATION + "-4-Thread4-"+BKPConstants.LedgerOpenRecoverReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    // here we are trying to write a fragment in a thread connected to the former BKProxy (BKP1) though it was already
                                    // recovery-opened and hence it should fail.
                                    +   BKPOPERATION + "-6-Thread1-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-4-300-"+BKPConstants.SF_ErrorFenced+"\n"
                                    +   BKPOPERATION + "-7-Thread1-"+BKPConstants.LedgerWriteCloseReq+"-ext1-"+BKPConstants.SF_OK+"\n";
        executeTestcase(testDefinition);
    }

    /**
     * In this testcase we try to read fragment which is in pending write, from another thread, which is connected to the same BKProxy, and it is expected to
     * fail
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void tryToReadOutOfOrderWritesInSameBKProxy() throws IOException, InterruptedException {
        String testDefinition =         BKPDETAILS + "-BKP1-5555\n"
                                    +   NUMOFTHREADS + "-3\n"
                                    +   THREADDETAILS + "-Thread1-BKP1\n"
                                    +   THREADDETAILS + "-Thread2-BKP1\n"
                                    +   THREADDETAILS + "-Thread3-BKP1\n"
                                    +   NUMOFSLOTS + "-10\n"
                                    +   BKPOPERATION + "-0-Thread1-"+BKPConstants.LedgerCreateReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-1-Thread1-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-1-1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-2-Thread2-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-2-20-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-3-Thread3-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-3-30000-"+BKPConstants.SF_OK+"\n"
                                    // this is out-of-order write
                                    +   BKPOPERATION + "-4-Thread1-"+BKPConstants.LedgerWriteEntryReq+"-true-false-ext1-5-300-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-5-Thread2-"+BKPConstants.LedgerOpenReadReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-6-Thread2-"+BKPConstants.LedgerReadEntryReq+"-ext1-2-1000-"+BKPConstants.SF_OK+"-20\n"
                                    +   BKPOPERATION + "-6-Thread3-"+BKPConstants.LedgerReadEntryReq+"-ext1-3-400000000-"+BKPConstants.SF_OK+"-30000\n"
                                    // entry 4 is never written, so it is supposed to fail
                                    +   BKPOPERATION + "-7-Thread2-"+BKPConstants.LedgerReadEntryReq+"-ext1-4-400000000-"+BKPConstants.SF_ErrorNoSuchFragment+"-3\n"
                                    // since entry 4 is not written, we shouldn't be able to read pending out-of-order
                                    +   BKPOPERATION + "-8-Thread2-"+BKPConstants.LedgerReadEntryReq+"-ext1-5-400000000-"+BKPConstants.SF_ErrorNoSuchFragment+"-3\n"
                                    +   BKPOPERATION + "-9-Thread2-"+BKPConstants.LedgerReadCloseReq+"-ext1-"+BKPConstants.SF_OK+"\n";
        executeTestcase(testDefinition);
    }

    /**
     * In this testcase we try to read fragment which is in pending write, from another thread, which is connected to the other BKProxy, and it is expected to
     * fail
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void outOfOrderWritesOpenReadInOtherBKProxy() throws IOException, InterruptedException {
        String testDefinition =         BKPDETAILS + "-BKP1-5555\n"
                                    +   BKPDETAILS + "-BKP2-7777\n"
                                    +   NUMOFTHREADS + "-6\n"
                                    +   THREADDETAILS + "-Thread1-BKP1\n"
                                    +   THREADDETAILS + "-Thread2-BKP1\n"
                                    +   THREADDETAILS + "-Thread3-BKP1\n"
                                    +   THREADDETAILS + "-Thread4-BKP2\n"
                                    +   THREADDETAILS + "-Thread5-BKP2\n"
                                    +   THREADDETAILS + "-Thread6-BKP2\n"
                                    +   NUMOFSLOTS + "-10\n"
                                    +   BKPOPERATION + "-0-Thread1-"+BKPConstants.LedgerCreateReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-1-Thread1-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-1-1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-2-Thread2-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-2-20-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-3-Thread3-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-3-30000-"+BKPConstants.SF_OK+"\n"
                                    // this is out-of-order write
                                    +   BKPOPERATION + "-4-Thread1-"+BKPConstants.LedgerWriteEntryReq+"-true-false-ext1-5-300-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-5-Thread4-"+BKPConstants.LedgerOpenReadReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-6-Thread4-"+BKPConstants.LedgerReadEntryReq+"-ext1-1-1000-"+BKPConstants.SF_OK+"-1\n"
                                    +   BKPOPERATION + "-6-Thread5-"+BKPConstants.LedgerReadEntryReq+"-ext1-2-1000-"+BKPConstants.SF_OK+"-20\n"
                                    +   BKPOPERATION + "-6-Thread6-"+BKPConstants.LedgerReadEntryReq+"-ext1-3-400000000-"+BKPConstants.SF_OK+"-30000\n"
                                    // entry 4 is never written, so it is supposed to fail
                                    +   BKPOPERATION + "-7-Thread4-"+BKPConstants.LedgerReadEntryReq+"-ext1-4-400000000-"+BKPConstants.SF_ErrorNoSuchFragment+"-3\n"
                                    // since entry 4 is not written, we shouldn't be able to read pending out-of-order entry even in thread connected to other BKProxy (BKP2)
                                    +   BKPOPERATION + "-8-Thread4-"+BKPConstants.LedgerReadEntryReq+"-ext1-5-400000000-"+BKPConstants.SF_ErrorNoSuchFragment+"-3\n"
                                    +   BKPOPERATION + "-9-Thread4-"+BKPConstants.LedgerReadCloseReq+"-ext1-"+BKPConstants.SF_OK+"\n";
        executeTestcase(testDefinition);
    }

    /**
     * In this testcase Ledger Write is closed even though there are pending writes, and then LedgerStat is called to
     * check if it returns correct value after discarding out-of-order writes
     *
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void ledgerStatAfterOutOfOrderWriteClose() throws IOException, InterruptedException {
        String testDefinition =         BKPDETAILS + "-BKP1-5555\n"
                                    +   NUMOFTHREADS + "-3\n"
                                    +   THREADDETAILS + "-Thread1-BKP1\n"
                                    +   THREADDETAILS + "-Thread2-BKP1\n"
                                    +   THREADDETAILS + "-Thread3-BKP1\n"
                                    +   NUMOFSLOTS + "-7\n"
                                    +   BKPOPERATION + "-0-Thread1-"+BKPConstants.LedgerCreateReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-1-Thread1-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-1-10-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-2-Thread2-"+BKPConstants.LedgerWriteEntryReq+"-true-false-ext1-3-10-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-3-Thread3-"+BKPConstants.LedgerWriteEntryReq+"-true-false-ext1-4-10-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-4-Thread1-"+BKPConstants.LedgerAsyncWriteEntryReq+"-ext1-5-20-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-5-Thread1-"+BKPConstants.LedgerWriteCloseReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    // since pending writes (fragment 3 and 4) are discarded after writeclose, now stat should return response 10
                                    +   BKPOPERATION + "-6-Thread1-"+BKPConstants.LedgerStatReq+"-ext1-"+BKPConstants.SF_OK+"-10\n";
        executeTestcase(testDefinition);
    }

    /*
     * Create two proxys, BKP1 and BKP2
     * BKP1 creates an extent ext1, writes, and close it. Makes sure it can stat it
     *
     * BKP2 makes sure it can stat ext1.
     */
    @Test
    public void statAtClusterLevel() throws IOException, InterruptedException {

        String testDefinition =         BKPDETAILS + "-BKP1-5555\n"
                                    +   BKPDETAILS + "-BKP2-7777\n"
                                    +   NUMOFTHREADS + "-2\n"
                                    +   THREADDETAILS + "-Thread1-BKP1\n"
                                    +   THREADDETAILS + "-Thread2-BKP2\n"
                                    +   NUMOFSLOTS + "-5\n"
                                    +   BKPOPERATION + "-0-Thread1-"+BKPConstants.LedgerCreateReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-1-Thread1-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-1-10-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-2-Thread1-"+BKPConstants.LedgerWriteCloseReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-3-Thread1-"+BKPConstants.LedgerStatReq+"-ext1-"+BKPConstants.SF_OK+"-10\n"
                                    +   BKPOPERATION + "-4-Thread2-"+BKPConstants.LedgerStatReq+"-ext1-"+BKPConstants.SF_OK+"-10\n";
        executeTestcase(testDefinition);
    }

    /**
     * In this testcase, in threads connected to other BKProxy, LedgerReadEntryReq is called without explicitly opening Ledger for Read. It is supposed to
     * open Ledger for Read with Recovery and function normally
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void outOfOrderWritesGetEntryImplicitOpenRecover() throws IOException, InterruptedException {
        String testDefinition =         BKPDETAILS + "-BKP1-5555\n"
                                    +   BKPDETAILS + "-BKP2-7777\n"
                                    +   NUMOFTHREADS + "-6\n"
                                    +   THREADDETAILS + "-Thread1-BKP1\n"
                                    +   THREADDETAILS + "-Thread2-BKP1\n"
                                    +   THREADDETAILS + "-Thread3-BKP1\n"
                                    +   THREADDETAILS + "-Thread4-BKP2\n"
                                    +   THREADDETAILS + "-Thread5-BKP2\n"
                                    +   THREADDETAILS + "-Thread6-BKP2\n"
                                    +   NUMOFSLOTS + "-11\n"
                                    +   BKPOPERATION + "-0-Thread1-"+BKPConstants.LedgerCreateReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-1-Thread1-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-1-1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-2-Thread2-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-2-20-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-3-Thread3-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-3-30000-"+BKPConstants.SF_OK+"\n"
                                    // this is out-of-order write
                                    +   BKPOPERATION + "-4-Thread1-"+BKPConstants.LedgerWriteEntryReq+"-true-false-ext1-5-300-"+BKPConstants.SF_OK+"\n"
                                    // in threads connected to other BKProxy (BKP2), without explicitly opening for Read, ReadEntry requests are made
                                    // concurrently. This will implicitly do LedgerOpenRecovery and it is supposed to be synchronized properly
                                    +   BKPOPERATION + "-5-Thread4-"+BKPConstants.LedgerReadEntryReq+"-ext1-1-1000-"+BKPConstants.SF_OK+"-1\n"
                                    +   BKPOPERATION + "-5-Thread5-"+BKPConstants.LedgerReadEntryReq+"-ext1-2-1000-"+BKPConstants.SF_OK+"-20\n"
                                    +   BKPOPERATION + "-5-Thread6-"+BKPConstants.LedgerReadEntryReq+"-ext1-3-400000000-"+BKPConstants.SF_OK+"-30000\n"
                                    // entry 4 is never written, so it is supposed to fail
                                    +   BKPOPERATION + "-6-Thread4-"+BKPConstants.LedgerReadEntryReq+"-ext1-4-400000000-"+BKPConstants.SF_ErrorNoSuchFragment+"-3\n"
                                    // since it was RecoveryOpen, out-of-order pending writes should be discarded. So reading entry 5 should fail
                                    +   BKPOPERATION + "-7-Thread4-"+BKPConstants.LedgerOpenRecoverReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    // The isClosed gets updated by means of a zk watcher asynchronously. So wait a little bit.
                                    +   BKPOPERATION + "-8-Thread2-"+Operation.SleepReq+"-1000\n"
                                    +   BKPOPERATION + "-9-Thread4-"+BKPConstants.LedgerReadEntryReq+"-ext1-5-400000000-"+BKPConstants.SF_ErrorNoSuchFragmentClosed+"-3\n"
                                    +   BKPOPERATION + "-10-Thread4-"+BKPConstants.LedgerReadCloseReq+"-ext1-"+BKPConstants.SF_OK+"\n";
        executeTestcase(testDefinition);
    }

    /*
     * Create two proxys, BKP1 and BKP2
     * BKP1 writes 4 entries and doesn't close the extent ext1.
     * BKP2 opens ext1 in readNoRecovery mode, successfully reads first 3 entries, and fails to read 4th entry.
     *      After waiting 10 secs it can successfully read 4th entry because explicitLAC should kick-in on the BKP1 context
     *      So BKP2 can read it after wait.
     */
    @Test
    public void readTail() throws IOException, InterruptedException {

        TestScenarioState.getCurrentTestScenarioState().getCommonBKPConfig().setExplictLacInterval(2);

        String testDefinition =         BKPDETAILS + "-BKP1-5555\n"
                                    +   BKPDETAILS + "-BKP2-7777\n"
                                    +   NUMOFTHREADS + "-2\n"
                                    +   THREADDETAILS + "-Thread1-BKP1\n"
                                    +   THREADDETAILS + "-Thread2-BKP2\n"
                                    +   NUMOFSLOTS + "-14\n"
                                    +   BKPOPERATION + "-0-Thread1-"+BKPConstants.LedgerCreateReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-1-Thread1-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-1-10-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-2-Thread1-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-2-10-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-3-Thread1-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-3-10-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-4-Thread1-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-4-10-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-5-Thread2-"+BKPConstants.LedgerOpenReadReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-6-Thread2-"+BKPConstants.LedgerReadEntryReq+"-ext1-1-1000-"+BKPConstants.SF_OK+"-10\n"
                                    +   BKPOPERATION + "-7-Thread2-"+BKPConstants.LedgerReadEntryReq+"-ext1-2-1000-"+BKPConstants.SF_OK+"-10\n"
                                    +   BKPOPERATION + "-8-Thread2-"+BKPConstants.LedgerReadEntryReq+"-ext1-3-1000-"+BKPConstants.SF_OK+"-10\n"
                                    +   BKPOPERATION + "-9-Thread2-"+BKPConstants.LedgerReadEntryReq+"-ext1-4-1000-"+BKPConstants.SF_ErrorNoSuchFragment+"-10\n"
                                    +   BKPOPERATION + "-10-Thread2-"+Operation.SleepReq+"-5000\n"
                                    +   BKPOPERATION + "-11-Thread2-"+BKPConstants.LedgerReadEntryReq+"-ext1-4-1000-"+BKPConstants.SF_OK+"-10\n"
                                    +   BKPOPERATION + "-12-Thread1-"+BKPConstants.LedgerWriteCloseReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-13-Thread2-"+BKPConstants.LedgerReadCloseReq+"-ext1-"+BKPConstants.SF_OK+"\n";

        executeTestcase(testDefinition);
    }

    /*
     * Just like tailRead() but continues the tail for few more entries.
     */
    @Test
    public void tailingRead() throws IOException, InterruptedException {

        TestScenarioState.getCurrentTestScenarioState().getCommonBKPConfig().setExplictLacInterval(2);

        String testDefinition =         BKPDETAILS + "-BKP1-5555\n"
                                    +   BKPDETAILS + "-BKP2-7777\n"
                                    +   NUMOFTHREADS + "-2\n"
                                    +   THREADDETAILS + "-Thread1-BKP1\n"
                                    +   THREADDETAILS + "-Thread2-BKP2\n"
                                    +   NUMOFSLOTS + "-18\n"
                                    +   BKPOPERATION + "-0-Thread1-"+BKPConstants.LedgerCreateReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-1-Thread1-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-1-10-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-2-Thread1-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-2-10-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-3-Thread1-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-3-10-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-4-Thread1-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-4-10-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-5-Thread2-"+BKPConstants.LedgerOpenReadReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-6-Thread2-"+BKPConstants.LedgerReadEntryReq+"-ext1-1-1000-"+BKPConstants.SF_OK+"-10\n"
                                    +   BKPOPERATION + "-7-Thread2-"+BKPConstants.LedgerReadEntryReq+"-ext1-2-1000-"+BKPConstants.SF_OK+"-10\n"
                                    +   BKPOPERATION + "-8-Thread2-"+BKPConstants.LedgerReadEntryReq+"-ext1-3-1000-"+BKPConstants.SF_OK+"-10\n"
                                    +   BKPOPERATION + "-9-Thread2-"+BKPConstants.LedgerReadEntryReq+"-ext1-4-1000-"+BKPConstants.SF_ErrorNoSuchFragment+"-10\n"
                                    +   BKPOPERATION + "-10-Thread2-"+Operation.SleepReq+"-5000\n"
                                    +   BKPOPERATION + "-11-Thread2-"+BKPConstants.LedgerReadEntryReq+"-ext1-4-1000-"+BKPConstants.SF_OK+"-10\n"
                                    +   BKPOPERATION + "-12-Thread1-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-5-10-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-13-Thread2-"+BKPConstants.LedgerReadEntryReq+"-ext1-5-1000-"+BKPConstants.SF_ErrorNoSuchFragment+"-10\n"
                                    +   BKPOPERATION + "-14-Thread2-"+Operation.SleepReq+"-5000\n"
                                    +   BKPOPERATION + "-15-Thread2-"+BKPConstants.LedgerReadEntryReq+"-ext1-5-1000-"+BKPConstants.SF_OK+"-10\n"
                                    +   BKPOPERATION + "-16-Thread1-"+BKPConstants.LedgerWriteCloseReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-17-Thread2-"+BKPConstants.LedgerReadCloseReq+"-ext1-"+BKPConstants.SF_OK+"\n";

        executeTestcase(testDefinition);
    }
    /*
     * Just like tailRead() but reads last entry twice  after close/reopen the ledger.
     */
    @Test
    public void readTailTwice() throws IOException, InterruptedException {

        TestScenarioState.getCurrentTestScenarioState().getCommonBKPConfig().setExplictLacInterval(2);

        String testDefinition =         BKPDETAILS + "-BKP1-5555\n"
                                    +   BKPDETAILS + "-BKP2-7777\n"
                                    +   NUMOFTHREADS + "-2\n"
                                    +   THREADDETAILS + "-Thread1-BKP1\n"
                                    +   THREADDETAILS + "-Thread2-BKP2\n"
                                    +   NUMOFSLOTS + "-17\n"
                                    +   BKPOPERATION + "-0-Thread1-"+BKPConstants.LedgerCreateReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-1-Thread1-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-1-10-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-2-Thread1-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-2-10-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-3-Thread1-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-3-10-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-4-Thread1-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-4-10-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-5-Thread2-"+BKPConstants.LedgerOpenReadReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-6-Thread2-"+BKPConstants.LedgerReadEntryReq+"-ext1-1-1000-"+BKPConstants.SF_OK+"-10\n"
                                    +   BKPOPERATION + "-7-Thread2-"+BKPConstants.LedgerReadEntryReq+"-ext1-2-1000-"+BKPConstants.SF_OK+"-10\n"
                                    +   BKPOPERATION + "-8-Thread2-"+BKPConstants.LedgerReadEntryReq+"-ext1-3-1000-"+BKPConstants.SF_OK+"-10\n"
                                    // Extent is not closed yet.
                                    +   BKPOPERATION + "-9-Thread2-"+BKPConstants.LedgerReadEntryReq+"-ext1-4-1000-"+BKPConstants.SF_ErrorNoSuchFragment+"-10\n"
                                    +   BKPOPERATION + "-10-Thread2-"+Operation.SleepReq+"-5000\n"
                                    +   BKPOPERATION + "-11-Thread2-"+BKPConstants.LedgerReadEntryReq+"-ext1-4-1000-"+BKPConstants.SF_OK+"-10\n"
                                    +   BKPOPERATION + "-12-Thread2-"+BKPConstants.LedgerReadCloseReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-13-Thread2-"+BKPConstants.LedgerOpenReadReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-14-Thread2-"+BKPConstants.LedgerReadEntryReq+"-ext1-4-1000-"+BKPConstants.SF_OK+"-10\n"
                                    +   BKPOPERATION + "-15-Thread1-"+BKPConstants.LedgerWriteCloseReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-16-Thread2-"+BKPConstants.LedgerReadCloseReq+"-ext1-"+BKPConstants.SF_OK+"\n";

        executeTestcase(testDefinition);
    }

    /*
     * This test simulates part of the retry_stream test of SDB. Basically a read and a write handle are opened on an extent
     * and the extent is written to, and we check that we can read the last fragment while the write handle is open and
     * when it gets closed.
     */
    @Test
    public void readTailTrailer() throws IOException, InterruptedException {

        TestScenarioState.getCurrentTestScenarioState().getCommonBKPConfig().setExplictLacInterval(20);
        String testDefinition =         BKPDETAILS + "-BKP1-5555\n"
                                    +   NUMOFSLOTS + "-16\n"
                                    +   NUMOFTHREADS + "-2\n"
                                    +   THREADDETAILS + "-Thread1-BKP1\n"
                                    +   THREADDETAILS + "-Thread2-BKP1\n"
                                    +   BKPOPERATION + "-0-Thread1-"+BKPConstants.LedgerCreateReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-1-Thread1-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-1-10-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-2-Thread1-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-2-10-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-3-Thread1-"+BKPConstants.LedgerOpenReadReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-4-Thread1-"+BKPConstants.LedgerReadEntryReq+"-ext1-1-1000-"+BKPConstants.SF_OK+"-10\n"
                                    +   BKPOPERATION + "-5-Thread1-"+BKPConstants.LedgerReadEntryReq+"-ext1-2-1000-"+BKPConstants.SF_OK+"-10\n"
                                    +   BKPOPERATION + "-6-Thread1-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-3-10-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-7-Thread1-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-4-10-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-8-Thread1-"+BKPConstants.LedgerReadEntryReq+"-ext1-3-1000-"+BKPConstants.SF_OK+"-10\n"
                                    +   BKPOPERATION + "-9-Thread1-"+BKPConstants.LedgerReadEntryReq+"-ext1-4-1000-"+BKPConstants.SF_OK+"-10\n"
                                    +   BKPOPERATION + "-10-Thread1-"+BKPConstants.LedgerWriteCloseReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-11-Thread1-"+BKPConstants.LedgerReadEntryReq+"-ext1-4-1000-"+BKPConstants.SF_OK+"-10\n"
                                    +   BKPOPERATION + "-12-Thread1-"+BKPConstants.LedgerReadCloseReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-13-Thread2-"+BKPConstants.LedgerOpenReadReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-14-Thread2-"+BKPConstants.LedgerReadEntryReq+"-ext1-4-1000-"+BKPConstants.SF_OK+"-10\n"
                                    +   BKPOPERATION + "-15-Thread2-"+BKPConstants.LedgerReadCloseReq+"-ext1-"+BKPConstants.SF_OK+"\n";
        executeTestcase(testDefinition);
    }

    /**
     * In this testcase we show how disabling retry of failed zk operations returns SF_ErrorMetaDataServer.
     * We cause the ledger create op, which talks to zk, to fail by artificially putting the zk server thread to
     * sleep. Without retires, the ledger create fails.
     *
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void zkClientSessionTimeOutTest() throws IOException, InterruptedException {
        // with the retries disabled, the ledger create op should fail
        TestScenarioState.getCurrentTestScenarioState().getCommonBKPConfig().setZkOpRetryCount(0);
        String testDefinition =         BKPDETAILS + "-BKP1-5555\n"
                                    +   NUMOFTHREADS + "-2\n"
                                    +   THREADDETAILS + "-Thread1-BKP1\n"
                                    +   THREADDETAILS + "-Thread2-BKP1\n"
                                    +   NUMOFSLOTS + "-1\n"
                                    +   PREOPSLEEP + "-1000\n"
                                    +   BKPOPERATION + "-0-Thread1-"+BKPConstants.LedgerCreateReq+"-ext1-"+BKPConstants.SF_ErrorMetaDataServer+"\n"
                                    +   BKPOPERATION + "-0-Thread2-"+Operation.ZkServerPauseOpReq+"-10000-"+"\n";
        executeTestcase(testDefinition);
    }

    /**
     * In this testcase we show how enabling retry of failed zk operations is useful. We cause the
     * ledger create op, which talks to zk, to fail by artificially putting the zk server thread to
     * sleep.When zk retry is enabled for such operations the ledger create operation succeeds.
     *
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void zkClientSessionTimeOutAndRetryTest() throws IOException, InterruptedException {
        // with the retries enabled the create op should go through
        TestScenarioState.getCurrentTestScenarioState().getCommonBKPConfig().setZkOpRetryCount(3);
        String testDefinition       =   BKPDETAILS + "-BKP1-5555\n"
                                    +   NUMOFTHREADS + "-2\n"
                                    +   THREADDETAILS + "-Thread1-BKP1\n"
                                    +   THREADDETAILS + "-Thread2-BKP1\n"
                                    +   NUMOFSLOTS + "-3\n"
                                    +   PREOPSLEEP + "-1000\n"
                                    +   BKPOPERATION + "-0-Thread1-"+BKPConstants.LedgerCreateReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-0-Thread2-"+Operation.ZkServerPauseOpReq+"-10000-"+"\n"
                                    +   BKPOPERATION + "-1-Thread1-"+BKPConstants.LedgerWriteCloseReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-2-Thread1-"+BKPConstants.LedgerListGetReq+"-ext1-"+BKPConstants.SF_OK+"-ext1-\n";
        executeTestcase(testDefinition);
    }

    /**
     * In this testcase an old proxy tries to delete an extent with a write handle which has been opened for
     * recovery in a new proxy.
     *
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void deleteRecoveredLedgerFromOldProxyWhichHasWriteHandle() throws IOException, InterruptedException {
        String testDefinition =         BKPDETAILS + "-BKP1-5555\n"
                                    +   BKPDETAILS + "-BKP2-7777\n"
                                    +   NUMOFTHREADS + "-2\n"
                                    +   THREADDETAILS + "-Thread1-BKP1\n"
                                    +   THREADDETAILS + "-Thread2-BKP2\n"
                                    +   NUMOFSLOTS + "-6\n"
                                    +   BKPOPERATION + "-0-Thread1-"+BKPConstants.LedgerCreateReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-1-Thread1-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-1-10-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-2-Thread1-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-2-20-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-3-Thread2-"+BKPConstants.LedgerOpenRecoverReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    // the following delete should succeed with a warning logged
                                    +   BKPOPERATION + "-4-Thread1-"+BKPConstants.LedgerDeleteReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-5-Thread2-"+BKPConstants.LedgerReadCloseReq+"-ext1-"+BKPConstants.SF_OK+"\n";
        executeTestcase(testDefinition);
    }


    public void executeTestcase(String testDefinition) throws IOException, InterruptedException {
        TestScenarioState currentTestScenario = TestScenarioState.getCurrentTestScenarioState();
        parseTestDefinition(testDefinition);
        Set<String> threadIds = currentTestScenario.getThreadIds();
        for (String threadId : threadIds) {
            BKPClientThread bkpThread = currentTestScenario.getBKPClientThread(threadId);
            bkpThread.setUncaughtExceptionHandler(threadExceptionHandler);
            bkpThread.start();
        }

        boolean areThreadsDone = currentTestScenario.getCurrentTestScenarioThreadCountDownLatch()
                .await(NUMOFSECSTOWAITFORCOMPLETION, TimeUnit.SECONDS);
        for (Operation failedOperation : currentTestScenario.getFailedOperations()) {
            currentTestScenarioExceptions.add(failedOperation.getOperationException());
        }
        for (String bkpThread : currentTestScenario.getThreadIds()) {
            Throwable threadException = currentTestScenario.getBKPClientThread(bkpThread).getThreadException();
            if (threadException != null) {
                currentTestScenarioExceptions.add(threadException);
            }
        }
        if (areThreadsDone) {
            if (!currentTestScenario.isScenarioDone()) {
                if (currentTestScenario.isScenarioFailed()) {
                    String exceptionMessages = "";
                    for (Operation failedOperation : currentTestScenario.getFailedOperations()) {
                        Exception e = failedOperation.getOperationException();
                        exceptionMessages += e.getMessage() + " \n";
                    }
                    Assert.fail(exceptionMessages);
                } else if (currentTestScenario.getCycBarrier().isBroken()) {
                    Assert.fail("CyclicBarrier is Broken at TimeSlot: " + currentTestScenario.getCurrentTimeSlot());
                } else {
                    Assert.fail("Scenario is not done successfully. The current timeslot: "
                            + currentTestScenario.getCurrentTimeSlot());
                }
            }
        } else {
            for (String bkpThread : currentTestScenario.getThreadIds()) {
                BKPClientThread bkpClientThread = currentTestScenario.getBKPClientThread(bkpThread);
                if (bkpClientThread.getState() != Thread.State.TERMINATED) {
                    currentTestScenarioExceptions.add(new OperationException(
                            String.format("ThreadId: %s has been blocked while doing Operation at Timeslot: %d",
                                    bkpClientThread.getThreadId(), currentTestScenario.getCurrentTimeSlot())));
                }
            }
            Assert.fail("All threads have not exited properly. The current timeslot: "
                    + currentTestScenario.getCurrentTimeSlot());
        }
    }

    public void parseTestDefinition(String testDefinition) throws IOException, NumberFormatException,
            InterruptedException {
        int preOpSleepMSecs = 0;
        short protocolVersion = BKPConstants.SFS_CURRENT_VERSION;
        String[] testDefinitionDetails = testDefinition.split(NEWLINE);
        TestScenarioState currentTestScenario = TestScenarioState.getCurrentTestScenarioState();
        for (int i = 0; i < testDefinitionDetails.length; i++) {
            String[] metadataDetails = testDefinitionDetails[i].split(SPLITREGEX);
            switch (metadataDetails[0]) {
            case BKPDETAILS:
                currentTestScenario.addAndStartBKP(metadataDetails[1], Integer.valueOf(metadataDetails[2]));
                break;
            case NUMOFTHREADS:
                currentTestScenario.setNumberOfThreads(Integer.valueOf(metadataDetails[1]));
                break;
            case THREADDETAILS:
                currentTestScenario.addBKPClientThread(metadataDetails[1], metadataDetails[2]);
                break;
            case NUMOFSLOTS:
                currentTestScenario.setNumberOfTimeSlots(Integer.valueOf(metadataDetails[1]));
                break;
            case PREOPSLEEP:
                preOpSleepMSecs = Integer.valueOf(metadataDetails[1]);
                break;
            case VERSION:
                protocolVersion = Short.valueOf(metadataDetails[1]);
                break;
            case BKPOPERATION:
                String operationDefinition = testDefinitionDetails[i].substring(testDefinitionDetails[i]
                        .indexOf(SPLITREGEX) + 1);
                Operation operation = AbstractOperation.build(protocolVersion, operationDefinition, this);
                if (preOpSleepMSecs > 0) {
                    operation.setPrePerformSleepMsecs(preOpSleepMSecs);
                    preOpSleepMSecs = 0;
                }

                currentTestScenario.addOperation(operation.getTimeSlot(), operation);
                break;
            }
        }
        int numOfThreadsMentioned = currentTestScenario.getCycBarrier().getParties();
        int actualNumOfThreadsDefined = currentTestScenario.getThreadIds().size();
        if (numOfThreadsMentioned != actualNumOfThreadsDefined) {
            throw new IllegalArgumentException("In the TestDefinition NumOfThreads specified: " + numOfThreadsMentioned
                    + " But actual NumOfThreads defined: " + actualNumOfThreadsDefined);
        }
    }
}
