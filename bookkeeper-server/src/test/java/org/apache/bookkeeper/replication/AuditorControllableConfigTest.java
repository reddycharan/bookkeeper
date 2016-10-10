package org.apache.bookkeeper.replication;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.replication.ReplicationException.BKAuditException;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.zookeeper.KeeperException;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AuditorControllableConfigTest extends BookKeeperClusterTestCase {
    public AuditorControllableConfigTest() {
        super(3);
    }

    @Before
    @Override
    public void setUp() throws Exception {
        baseConf.setAutoRecoveryDaemonEnabled(false);
        super.setUp();
    }

    private final static Logger LOG = LoggerFactory.getLogger(AuditorControllableConfigTest.class);

    /**
     * testcase to validate that if AuditorPeriodicCheckOkToRunNow is set to false, then AuditorPeriodicCheck will be
     * scheduled to run after AuditorPeriodicCheckRetryInterval. If it is enabled then next execution will be executed
     * after AuditorPeriodicCheckInterval but not after AuditorPeriodicCheckRetryInterval
     * 
     * @throws Exception
     */
    @Test(timeout = 60000)
    public void testPeriodicCheckControllableConfig() throws Exception {
        final int numLedgers = 10;
        List<Long> ids = new LinkedList<Long>();
        for (int i = 0; i < numLedgers; i++) {
            LedgerHandle lh = bkc.createLedger(3, 3, DigestType.CRC32, "passwd".getBytes());
            ids.add(lh.getId());
            for (int j = 0; j < 2; j++) {
                lh.addEntry("testdata".getBytes());
            }
            lh.close();
        }

        long interval = 5;
        long retryInterval = 1;
        ServerConfiguration auditorConfig = bsConfs.get(0);
        auditorConfig.setAuditorPeriodicCheckInterval(interval);
        auditorConfig.setAuditorPeriodicCheckRetryInterval(retryInterval);
        auditorConfig.setAuditorPeriodicCheckOkToRunNow(false);
        AtomicInteger checkAllLedgersCalled = new AtomicInteger();

        Auditor auditor = new Auditor(Bookie.getBookieAddress(bsConfs.get(0)).toString(), auditorConfig, zkc,
                NullStatsLogger.INSTANCE) {
            @Override
            void checkAllLedgers()
                    throws BKAuditException, BKException, IOException, InterruptedException, KeeperException {
                checkAllLedgersCalled.incrementAndGet();
                super.checkAllLedgers();
            }
        };
        auditor.start();

        // AuditorPeriodicCheckOkToRunNow is disabled, so checkAllLedgers shouldn't be called yet
        Thread.sleep((interval + 1) * 1000);
        assertTrue("AuditorPeriodicCheckOkToRunNow is disabled, so checkAllLedgers shouldn't be called yet, but it is called - "
                + checkAllLedgersCalled.intValue(), (checkAllLedgersCalled.intValue() == 0));

        // AuditorPeriodicCheckOkToRunNow is enabled, so checkAllLedgers should have been called only once after retryInterval
        auditorConfig.setAuditorPeriodicCheckOkToRunNow(true);
        Thread.sleep((retryInterval + 1) * 1000);
        assertTrue(
                "AuditorPeriodicCheckOkToRunNow is enabled, so checkAllLedgers should have been called only once, but it is called - "
                        + checkAllLedgersCalled.intValue(),
                (checkAllLedgersCalled.intValue() == 1));

        // we just waited for retryInterval, so checkAllLedgers should not have been called for second execution
        Thread.sleep((retryInterval + 1) * 1000);
        assertTrue(
                "we just waited for retryInterval, so checkAllLedgers should not have been called for second execution, but it is called - "
                        + checkAllLedgersCalled.intValue(),
                (checkAllLedgersCalled.intValue() == 1));

        // we waited for interval, so checkAllLedgers should have been called for second execution
        Thread.sleep((interval - (retryInterval + 1) + 1) * 1000);
        assertTrue(
                "we waited for interval, so checkAllLedgers should have been called for second execution, but it is called - "
                        + checkAllLedgersCalled.intValue(),
                (checkAllLedgersCalled.intValue() == 2));

    }

    /**
     * testcase to validate that if AuditorPeriodicBookieCheckOkToRunNow is set to false, then AuditorPeriodicBookieCheck
     * will be scheduled to run after AuditorPeriodicBookieCheckRetryInterval. If it is enabled then next execution will
     * be executed after AuditorPeriodicBookieCheckInterval but not after AuditorPeriodicBookieCheckRetryInterval
     * 
     * @throws Exception
     */
    @Test(timeout = 60000)
    public void testPeriodicBookieCheckControllableConfig() throws Exception {
        final int numLedgers = 10;
        List<Long> ids = new LinkedList<Long>();
        for (int i = 0; i < numLedgers; i++) {
            LedgerHandle lh = bkc.createLedger(3, 3, DigestType.CRC32, "passwd".getBytes());
            ids.add(lh.getId());
            for (int j = 0; j < 2; j++) {
                lh.addEntry("testdata".getBytes());
            }
            lh.close();
        }

        long interval = 5;
        long retryInterval = 1;
        ServerConfiguration auditorConfig = bsConfs.get(0);
        auditorConfig.setAuditorPeriodicBookieCheckInterval(interval);
        auditorConfig.setAuditorPeriodicBookieCheckRetryInterval(retryInterval);
        auditorConfig.setAuditorPeriodicBookieCheckOkToRunNow(false);
        AtomicInteger startAuditCalled = new AtomicInteger();

        Auditor auditor = new Auditor(Bookie.getBookieAddress(bsConfs.get(0)).toString(), auditorConfig, zkc,
                NullStatsLogger.INSTANCE) {
            @Override
            void startAudit(boolean shutDownTask) {
                startAuditCalled.incrementAndGet();
                super.startAudit(shutDownTask);
            }
        };
        auditor.start();

        Thread.sleep(500);
        assertTrue(
                "BookieCheck should have been executed as part of startup though AuditorPeriodicBookieCheckOkToRunNow is disabled, but it is called - "
                        + startAuditCalled.intValue(),
                (startAuditCalled.intValue() == 1));
        
        // AuditorPeriodicBookieCheckOkToRunNow is disabled, so startAudit shouldn't be called any more
        Thread.sleep((interval + 1) * 1000);
        assertTrue("AuditorPeriodicBookieCheckOkToRunNow is disabled, so startAudit shouldn't be called any more, but it is called - "
                + startAuditCalled.intValue(), (startAuditCalled.intValue() == 1));

        // AuditorPeriodicBookieCheckOkToRunNow is enabled, so startAudit should have been called once more after retryInterval
        auditorConfig.setAuditorPeriodicBookieCheckOkToRunNow(true);
        Thread.sleep((retryInterval + 1) * 1000);
        assertTrue(
                "AuditorPeriodicBookieCheckOkToRunNow is enabled, so startAudit should have been called once more after retryInterval, but it is called - "
                        + startAuditCalled.intValue(),
                (startAuditCalled.intValue() == 2));

        // we just waited for retryInterval, so startAudit should not have been called for next execution
        Thread.sleep((retryInterval + 1) * 1000);
        assertTrue(
                "we just waited for retryInterval, so startAudit should not have been called for next execution, but it is called - "
                        + startAuditCalled.intValue(),
                (startAuditCalled.intValue() == 2));

        // we waited for interval, so startAudit should have been called for next execution
        Thread.sleep((interval - (retryInterval + 1) + 1) * 1000);
        assertTrue(
                "we waited for interval, so startAudit should have been called for next execution, but it is called - "
                        + startAuditCalled.intValue(),
                (startAuditCalled.intValue() == 3));
    }
    
    /**
     * testcase to validate that if AuditorPeriodicBookieCheck is disabled by setting AuditorPeriodicBookieCheckInterval to 0, 
     * then AuditorPeriodicBookieCheck will run once at the startup but not later though AuditorPeriodicBookieCheckOkToRunNow
     * is true
     * 
     * @throws Exception
     */
    @Test(timeout = 60000)
    public void testPeriodicBookieCheckWhenItIsDisabled() throws Exception {
        final int numLedgers = 10;
        List<Long> ids = new LinkedList<Long>();
        for (int i = 0; i < numLedgers; i++) {
            LedgerHandle lh = bkc.createLedger(3, 3, DigestType.CRC32, "passwd".getBytes());
            ids.add(lh.getId());
            for (int j = 0; j < 2; j++) {
                lh.addEntry("testdata".getBytes());
            }
            lh.close();
        }

        long retryInterval = 1;
        ServerConfiguration auditorConfig = bsConfs.get(0);
        auditorConfig.setAuditorPeriodicBookieCheckInterval(0);
        auditorConfig.setAuditorPeriodicBookieCheckRetryInterval(retryInterval);
        auditorConfig.setAuditorPeriodicBookieCheckOkToRunNow(true);
        AtomicInteger startAuditCalled = new AtomicInteger();

        Auditor auditor = new Auditor(Bookie.getBookieAddress(bsConfs.get(0)).toString(), auditorConfig, zkc,
                NullStatsLogger.INSTANCE) {
            @Override
            void startAudit(boolean shutDownTask) {
                startAuditCalled.incrementAndGet();
                super.startAudit(shutDownTask);
            }
        };
        auditor.start();
        
        Thread.sleep(500);
        assertTrue(
                "BookieCheck should have been executed as part of startup though setAuditorPeriodicBookieCheckInterval is 0, but it is called - "
                        + startAuditCalled.intValue(),
                (startAuditCalled.intValue() == 1));
        
        // AuditorPeriodicBookieCheck is disabled, so startAudit shouldn't be called any more
        Thread.sleep(6000);
        assertTrue("AuditorPeriodicBookieCheck is disabled, so startAudit shouldn't be called any more, but it is called - "
                + startAuditCalled.intValue(), (startAuditCalled.intValue() == 1));
    }
}
