package org.apache.bookkeeper.perf;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.apache.bookkeeper.BKExtentId;
import org.apache.bookkeeper.BKExtentIdByteArray;
import org.apache.bookkeeper.BKExtentLedgerMap;
import org.apache.bookkeeper.BKSfdcClient;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.conf.BookKeeperProxyConfiguration;
import org.apache.bookkeeper.perf.End2EndPerfTest.TestScenario;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.util.concurrent.RateLimiter;

/*
 * Test scenario that simulates sequential read of ledgers via BK client.
 * It picks random ledger and reads it from first to the last entry. 
 */
public class SfdcClientSequentialReadScenario extends TestScenario {
    static final Logger LOG = LoggerFactory.getLogger(SfdcClientWriteScenario.class);
    
    public SfdcClientSequentialReadScenario(String scenarioName, Properties props, MetricRegistry metrics,
            RateLimiter throttler) {
        super(scenarioName, props, metrics, throttler);
    }

    final Random random = new SecureRandom();
    
    BookKeeperProxyConfiguration bkConfig;
    String configFile;

    int numberOfLedgers;

    BookKeeper bk;
    BKExtentLedgerMap elm;
    BKSfdcClient bkSfdc;
    
    BKExtentId extId = null;
    int fragmentId = 0;
    int lac = -1;

    Timer readLatency;
    Meter bytesRate;

    @Override
    public void setUp() throws Exception {
        bkConfig = new BookKeeperProxyConfiguration();
        configFile = props.getProperty("bkconfig", "conf/bk_client_proxy.conf");
        bkConfig.loadConf(new File(configFile).toURI().toURL());

        numberOfLedgers = Integer.parseInt(props.getProperty("numLedgers", "25"));

        bk = new BookKeeper(bkConfig);
        elm = BKExtentLedgerMap.newBuilder()
                .setReadHandleCacheLen(bkConfig.getReadHandleCacheLen())
                .setReadHandleTTL(bkConfig.getReadHandleTTL())
                .build();
        bkSfdc = new BKSfdcClient(bkConfig, bk, elm, null, null);

        readLatency = metrics.timer(buildMetricName("readLatency"));
        bytesRate = metrics.meter(buildMetricName("bytesRate"));
    }

    @Override
    public void tearDown() {
        try {
            bk.close();
        } catch (Exception e) {
            LOG.error("unexpected exception on bk.close()", e);
        }
    }

    @Override
    public boolean next() throws Exception {
        if (extId == null || fragmentId > lac) {
            numberOfLedgers--;
            if(numberOfLedgers < 0) {
                return false;
            }
            
            extId = selectNextLedger();
            if(extId == null) {
                LOG.warn("no ledger to read from, exiting");
                return false;
            }
            lac = bkSfdc.ledgerLac(extId);
            fragmentId = 1;
        }

        final Timer.Context ct = readLatency.time();
        try {
            ByteBuffer data = bkSfdc.ledgerGetEntry(extId, fragmentId, 0);
            data.flip();
            bytesRate.mark(data.limit());
        } catch (Exception e) {
            LOG.error("Exception while reading entry", e);
            return false;
        } finally {
            ct.stop();
        }
        fragmentId++;
        return true;
    }

    private BKExtentId selectNextLedger() throws IOException {
        List<Long> extents = new ArrayList<>(1024);
        bkSfdc.ledgerList().forEach(extents::add);
        
        if(extents.size() < 1) {
            return null;
        }
        long ledger = extents.get(random.nextInt(extents.size()));
        return new BKExtentIdByteArray(ledger);
    }

}
