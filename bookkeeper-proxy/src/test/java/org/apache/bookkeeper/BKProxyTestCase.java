package org.apache.bookkeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class BKProxyTestCase extends BookKeeperClusterTestCase {

    public BKProxyTestCase() {
        super(3);
    }

    public static final String SPLITREGEX = "-";
    public static final String NEWLINE = "\n";
    public static final String BKPDETAILS = "BKProxyDetails";
    public static final String NUMOFTHREADS = "NumOfThreads";
    public static final String THREADDETAILS = "ThreadDetails";
    public static final String NUMOFSLOTS = "NumOfSlots";
    public static final String BKPOPERATION = "BKPOperation";
    public static final int NUMOFSECSTOWAITFORCOMPLETION = 30;

    @Before
    public void testcaseSetup() throws InterruptedException {
        BKPClientThread.timeoutDurationInSecs = 4;
        TestScenarioState.instantiateCurrentTestScenarioState();
        BKProxyMain.bkserver = zkUtil.getZooKeeperConnectString();
    }

    @After
    public void testcaseCleanup() {
        TestScenarioState currentScenario = TestScenarioState.getCurrentTestScenarioState();
        currentScenario.closeAllClientSocketChannels();
        currentScenario.shutDownAllBkProxies();
        TestScenarioState.clearCurrentTestScenarioState();
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
                                    +   NUMOFSLOTS + "-4\n"
                                    +   BKPOPERATION + "-0-Thread1-"+BKPConstants.LedgerCreateReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-1-Thread1-"+BKPConstants.LedgerWriteCloseReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-2-Thread1-"+BKPConstants.LedgerOpenReadReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-3-Thread1-"+BKPConstants.LedgerReadCloseReq+"-ext1-"+BKPConstants.SF_OK+"\n";
        executeTestcase(testDefinition, "basicTestWithLedgerCreateAndClose");
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
                                    +   BKPOPERATION + "-5-Thread1-"+BKPConstants.LedgerListGetReq+"-ext1-"+BKPConstants.SF_OK+"-:-\n"
                                    +   BKPOPERATION + "-6-Thread1-"+BKPConstants.LedgerDeleteAllReq+"-ext1-"+BKPConstants.SF_OK+"\n";
        executeTestcase(testDefinition, "basicLedgerDeleteAllTest");
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
        executeTestcase(testDefinition, "basicLedgerListGetReqTest");
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
                                    +   BKPOPERATION + "-3-Thread1-"+BKPConstants.LedgerOpenReadReq+"-extn-"+BKPConstants.SF_ErrorNotFound+"\n"
                                    +   BKPOPERATION + "-4-Thread1-"+BKPConstants.LedgerOpenReadReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-5-Thread1-"+BKPConstants.LedgerReadCloseReq+"-extn-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-6-Thread1-"+BKPConstants.LedgerReadCloseReq+"-extn-"+BKPConstants.SF_OK+"\n";
        executeTestcase(testDefinition, "basicTestWithLedgerCreateAndNonExistingClose");
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
                                    +   NUMOFSLOTS + "-10\n"
                                    +   BKPOPERATION + "-0-Thread1-"+BKPConstants.LedgerCreateReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-1-Thread1-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-1-1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-2-Thread2-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-2-20-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-3-Thread3-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-3-30000-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-4-Thread1-"+BKPConstants.LedgerWriteCloseReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-5-Thread1-"+BKPConstants.LedgerOpenReadReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-6-Thread1-"+BKPConstants.LedgerReadEntryReq+"-ext1-1-1000-"+BKPConstants.SF_OK+"-1\n"
                                    +   BKPOPERATION + "-7-Thread2-"+BKPConstants.LedgerReadEntryReq+"-ext1-2-1000-"+BKPConstants.SF_OK+"-20\n"
                                    +   BKPOPERATION + "-8-Thread3-"+BKPConstants.LedgerReadEntryReq+"-ext1-3-400000000-"+BKPConstants.SF_OK+"-30000\n"
                                    +   BKPOPERATION + "-9-Thread1-"+BKPConstants.LedgerReadCloseReq+"-ext1-"+BKPConstants.SF_OK+"\n";
        executeTestcase(testDefinition, "simpleWriteAndReadLedger");
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
                                    +   BKPOPERATION + "-5-Thread3-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-4-30000-"+BKPConstants.SF_InternalError+"\n";
        executeTestcase(testDefinition, "writeLedgerAfterWriteClose");
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
                                    +   BKPOPERATION + "-5-Thread3-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-4-30000-"+BKPConstants.SF_InternalError+"\n";
        executeTestcase(testDefinition, "writeLedgerAfterFragment0");
    }

    /**
     * In this testcase we try to write extent of size greater than MAX_FRAG_SIZE and validate the error response
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void tryWritingOversizedFragment() throws IOException, InterruptedException {
        //Increasing TimeoutDuration because it might take longer time to create and send fragment of size BKPConstants.MAX_FRAG_SIZE
        BKPClientThread.timeoutDurationInSecs = 8;
        String testDefinition =         BKPDETAILS + "-BKP1-5555\n"
                                    +   NUMOFTHREADS + "-3\n"
                                    +   THREADDETAILS + "-Thread1-BKP1\n"
                                    +   THREADDETAILS + "-Thread2-BKP1\n"
                                    +   THREADDETAILS + "-Thread3-BKP1\n"
                                    +   NUMOFSLOTS + "-11\n"
                                    +   BKPOPERATION + "-0-Thread1-"+BKPConstants.LedgerCreateReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-1-Thread1-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-1-"+(BKPConstants.MAX_FRAG_SIZE+100)+"-"+BKPConstants.SF_ErrorBadRequest+"\n"
                                    +   BKPOPERATION + "-2-Thread1-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-1-100-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-3-Thread2-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-2-20-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-4-Thread3-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-3-30000-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-5-Thread1-"+BKPConstants.LedgerWriteCloseReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-6-Thread1-"+BKPConstants.LedgerOpenReadReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-7-Thread1-"+BKPConstants.LedgerReadEntryReq+"-ext1-1-"+(BKPConstants.MAX_FRAG_SIZE)+"-"+BKPConstants.SF_OK+"-100\n"
                                    +   BKPOPERATION + "-8-Thread2-"+BKPConstants.LedgerReadEntryReq+"-ext1-2-1000-"+BKPConstants.SF_OK+"-20\n"
                                    +   BKPOPERATION + "-9-Thread3-"+BKPConstants.LedgerReadEntryReq+"-ext1-3-400000000-"+BKPConstants.SF_OK+"-30000\n"
                                    +   BKPOPERATION + "-10-Thread1-"+BKPConstants.LedgerReadCloseReq+"-ext1-"+BKPConstants.SF_OK+"\n";
        executeTestcase(testDefinition, "tryWritingOversizedFragment");
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
                                    +   BKPOPERATION + "-2-Thread1-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-1-10-"+BKPConstants.SF_ErrorNotFound+"\n"
                                    +   BKPOPERATION + "-3-Thread1-"+BKPConstants.LedgerWriteCloseReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-4-Thread2-"+BKPConstants.LedgerListGetReq+"-ext1-"+BKPConstants.SF_OK+"-:-\n"
                                    +   BKPOPERATION + "-5-Thread3-"+BKPConstants.LedgerDeleteReq+"-ext1-"+BKPConstants.SF_ErrorNotFound+"\n"
                                    +   BKPOPERATION + "-6-Thread2-"+BKPConstants.LedgerListGetReq+"-ext1-"+BKPConstants.SF_OK+"-:-\n";
        executeTestcase(testDefinition, "ledgerDeleteTest");
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
                                    +   BKPOPERATION + "-4-Thread1-"+BKPConstants.LedgerOpenReadReq+"-ext1-"+BKPConstants.SF_ErrorNotFound+"\n";
        executeTestcase(testDefinition, "ledgerDeleteTestAfterWriteClose");
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
                                    +   BKPOPERATION + "-0-Thread1-"+BKPConstants.LedgerDeleteReq+"-ext1-"+BKPConstants.SF_ErrorNotFound+"\n"
                                    +   BKPOPERATION + "-1-Thread1-"+BKPConstants.LedgerListGetReq+"-ext1-"+BKPConstants.SF_OK+"-:-\n";
        executeTestcase(testDefinition, "nonExistingledgerDeleteTest");
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
                                    +   BKPOPERATION + "-2-Thread2-"+BKPConstants.LedgerWriteEntryReq+"-true-true-extn-2-10-"+BKPConstants.SF_ErrorNotFound+"\n"
                                    +   BKPOPERATION + "-3-Thread3-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-2-10-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-4-Thread1-"+BKPConstants.LedgerWriteCloseReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-5-Thread1-"+BKPConstants.LedgerOpenReadReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-6-Thread1-"+BKPConstants.LedgerReadEntryReq+"-ext1-1-1000-"+BKPConstants.SF_OK+"-10\n"
                                    +   BKPOPERATION + "-7-Thread2-"+BKPConstants.LedgerReadEntryReq+"-extn-2-1000-"+BKPConstants.SF_ErrorNotFound+"\n"
                                    +   BKPOPERATION + "-8-Thread3-"+BKPConstants.LedgerReadEntryReq+"-ext1-9-1000-"+BKPConstants.SF_ErrorNotFound+"-10\n"
                                    +   BKPOPERATION + "-9-Thread1-"+BKPConstants.LedgerReadEntryReq+"-ext1-2-1000-"+BKPConstants.SF_OK+"-10\n"
                                    +   BKPOPERATION + "-10-Thread1-"+BKPConstants.LedgerReadCloseReq+"-ext1-"+BKPConstants.SF_OK+"\n";
        executeTestcase(testDefinition, "simpleWriteAndReadLedgerForNonExistingEntries");
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
                                    +   BKPOPERATION + "-2-Thread1-"+BKPConstants.LedgerWriteCloseReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-3-Thread2-"+BKPConstants.LedgerDeleteAllReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-4-Thread3-"+BKPConstants.LedgerListGetReq+"-ext1-"+BKPConstants.SF_OK+"-:-\n";;
        executeTestcase(testDefinition, "concurrentWrites");
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
        executeTestcase(testDefinition, "simpleWriteAndConcurrentReads");
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
        executeTestcase(testDefinition, "outOfOrderWrites");
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
        executeTestcase(testDefinition, "concurrentWriteAndReads");
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
        executeTestcase(testDefinition, "testingLedgerLengthUsingWriteHandle");
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
        executeTestcase(testDefinition, "testingLedgerLengthUsingOpenedReadHandle");
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
        executeTestcase(testDefinition, "testingLedgerLengthUsingReadHandle");
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
        executeTestcase(testDefinition, "multipleLedgersWrites");
    }

    /**
     * In this testcase multiple BookKeeper proxies are created and multiple ledgers are written concurrently
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void multipleExtentsWritesUsingMultipleBKProxies() throws IOException, InterruptedException {
        String testDefinition =         BKPDETAILS + "-BKP1-5555\n"
                                    +   BKPDETAILS + "-BKP2-6666\n"
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
                                    +   BKPOPERATION + "-2-Thread2-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-2-10-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-2-Thread5-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext2-2-10-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-3-Thread3-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext1-3-10-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-3-Thread6-"+BKPConstants.LedgerWriteEntryReq+"-true-true-ext2-3-10-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-4-Thread1-"+BKPConstants.LedgerWriteCloseReq+"-ext1-"+BKPConstants.SF_OK+"\n"
                                    +   BKPOPERATION + "-4-Thread4-"+BKPConstants.LedgerWriteCloseReq+"-ext2-"+BKPConstants.SF_OK+"\n";
        executeTestcase(testDefinition, "multipleExtentsWritesUsingMultipleBKProxies");
    }

    /**
     * In this testcase multiple BookKeeper proxies are created and multiple ledgers are written and read concurrently
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void multipleExtentsWritesAndReadsUsingMultipleBKProxies() throws IOException, InterruptedException {
        String testDefinition =         BKPDETAILS + "-BKP1-5555\n"
                                    +   BKPDETAILS + "-BKP2-6666\n"
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
                                    +   BKPOPERATION + "-9-Thread2-"+BKPConstants.LedgerListGetReq+"-ext1-"+BKPConstants.SF_OK+"-ext2-\n"
                                    +   BKPOPERATION + "-9-Thread4-"+BKPConstants.LedgerListGetReq+"-ext1-"+BKPConstants.SF_OK+"-ext2-\n";
        executeTestcase(testDefinition, "multipleExtentsWritesAndReadsUsingMultipleBKProxies");
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
        executeTestcase(testDefinition, "readLedgerBeforeWriteClose");
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
        executeTestcase(testDefinition, "simpleWriteAndReadFragment0");
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
        executeTestcase(testDefinition, "simpleWriteFragment0AndReadFragment0");
    }

    /**
     * In this testcase, we try to read Fragment 0 without writeclosing, which should return SF_ErrorNotFound return status
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
                                    +   BKPOPERATION + "-7-Thread3-"+BKPConstants.LedgerReadEntryReq+"-ext1-0-400000000-"+BKPConstants.SF_ErrorNotFound+"-30000\n";
        executeTestcase(testDefinition, "readFragment0WithoutWriteClose");
    }

    public void executeTestcase(String testDefinition, String testcaseName) throws IOException, InterruptedException {
        TestScenarioState currentTestScenario = TestScenarioState.getCurrentTestScenarioState();
        parseTestDefinition(testDefinition);
        Set<String> threadIds = currentTestScenario.getThreadIds();
        List<Thread> bkpClientThreadInstances = new ArrayList<Thread>();
        for (String threadId : threadIds) {
            Thread actualThread = new Thread(currentTestScenario.getBKPClientThread(threadId));
            bkpClientThreadInstances.add(actualThread);
            actualThread.start();
        }

        boolean areThreadsDone = currentTestScenario.getCurrentTestScenarioThreadCountDownLatch().await(
                NUMOFSECSTOWAITFORCOMPLETION, TimeUnit.SECONDS);
        if (areThreadsDone) {
            if (!currentTestScenario.isScenarioDone()) {
                if (currentTestScenario.isScenarioFailed()) {
                    String exceptionMessages = "";
                    for (BKPOperation failedOperation : currentTestScenario.getFailedOperations()) {
                        Exception e = failedOperation.getOperationException();
                        exceptionMessages += e.getMessage() + " ";
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
            Assert.fail("All threads have not exited properly. The current timeslot: "
                    + currentTestScenario.getCurrentTimeSlot());
        }
        System.out.println("Successfully Completed executing Testcase: " + testcaseName);
    }

    public void parseTestDefinition(String testDefinition) throws IOException, NumberFormatException,
            InterruptedException {
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
            case BKPOPERATION:
                String bkpOperationDefinition = testDefinitionDetails[i].substring(testDefinitionDetails[i]
                        .indexOf(SPLITREGEX) + 1);
                BKPOperation bkpOperation = BKPOperation.build(bkpOperationDefinition);
                currentTestScenario.addBKPOperation(bkpOperation.getTimeSlot(), bkpOperation);
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
