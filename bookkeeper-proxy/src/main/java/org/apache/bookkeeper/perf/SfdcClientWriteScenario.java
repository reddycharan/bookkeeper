package org.apache.bookkeeper.perf;

import java.io.File;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.Properties;

import org.apache.bookkeeper.BKExtentId;
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
 * Test scenario that simulates synchronous writes via BK client.
 */
public class SfdcClientWriteScenario extends TestScenario {
    static final Logger LOG = LoggerFactory.getLogger(SfdcClientWriteScenario.class);
    
    public SfdcClientWriteScenario(String scenarioName, Properties props, MetricRegistry metrics,
            RateLimiter throttler) {
        super(scenarioName, props, metrics, throttler);
    }

    int entrySize;
    byte[] data;
    ByteBuffer buffData;

    BookKeeperProxyConfiguration bkConfig;
    String configFile;

    int numberOfLedgers;
    long maxSizeBytes;
    long maxEntries;

    BookKeeper bk;
    BKExtentLedgerMap elm;
    BKSfdcClient bkSfdc;
    
    BKExtentId extId = null;
    int fragmentId = 0;
    long written =  0;
    long ledgers = 0;

    Timer writeLatency;
    Meter bytesRate;

    @Override
    public void setUp() throws Exception {
        bkConfig = new BookKeeperProxyConfiguration();
        configFile = props.getProperty("bkconfig", "conf/bk_client_proxy.conf");
        bkConfig.loadConf(new File(configFile).toURI().toURL());

        entrySize = Integer.parseInt(props.getProperty("entrySize", "65536"));
        data = new byte[entrySize];
        (new SecureRandom()).nextBytes(data);
        buffData = ByteBuffer.wrap(data);

        numberOfLedgers = Integer.parseInt(props.getProperty("numLedgers", "25"));
        maxSizeBytes = Long.parseLong(props.getProperty("maxLedgerSizeMB", "2048")) * 1024 * 1024;
        maxEntries = maxSizeBytes / entrySize;

        bk = new BookKeeper(bkConfig);
        elm = BKExtentLedgerMap.newBuilder()
                .setReadHandleCacheLen(bkConfig.getReadHandleCacheLen())
                .setReadHandleTTL(bkConfig.getReadHandleTTL())
                .build();
        bkSfdc = new BKSfdcClient(bkConfig, bk, elm, null, null);

        writeLatency = metrics.timer(buildMetricName("writeLatency"));
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
        if(extId == null) {
            extId = bkSfdc.ledgerCreate();
            written = 0;
            fragmentId = 0;
            ledgers++;
        }
        
        buffData.rewind();
        final Timer.Context ct = writeLatency.time();
        try {
            bkSfdc.ledgerPutEntry(extId, ++fragmentId, buffData, null, null);
        } finally {
            ct.stop();
        }
        bytesRate.mark(data.length);
        written += data.length;
        
        if(written > maxSizeBytes - entrySize) {
            bkSfdc.ledgerWriteClose(extId);
            extId = null;
        }
        
        return ledgers < numberOfLedgers;
    }

}
