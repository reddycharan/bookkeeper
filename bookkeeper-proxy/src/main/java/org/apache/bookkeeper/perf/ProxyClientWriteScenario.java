package org.apache.bookkeeper.perf;

import java.security.SecureRandom;
import java.util.Properties;

import org.apache.bookkeeper.BKPConstants;
import org.apache.bookkeeper.perf.End2EndPerfTest.TestScenario;
import org.apache.bookkeeper.proxyclient.BKProxyClient;
import org.apache.bookkeeper.proxyclient.ReturnValue.CreateExtentReturnValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.util.concurrent.RateLimiter;

/*
 * Test scenario that simulates synchronous writes via BK Proxy.
 */
public class ProxyClientWriteScenario extends TestScenario {
    static final Logger LOG = LoggerFactory.getLogger(ProxyClientWriteScenario.class);
    
    public ProxyClientWriteScenario(String scenarioName, Properties props, MetricRegistry metrics,
            RateLimiter throttler) {
        super(scenarioName, props, metrics, throttler);
    }

    int entrySize;
    byte[] data;

    int numberOfLedgers;
    long maxSizeBytes;
    long maxEntries;

    BKProxyClient proxy;
    
    byte[] extId = null;
    int fragmentId = 0;
    long written =  0;
    long ledgers = 0;

    Timer writeLatency;
    Meter bytesRate;

    @Override
    public void setUp() throws Exception {
        entrySize = Integer.parseInt(props.getProperty("entrySize", "65536"));
        data = new byte[entrySize];
        (new SecureRandom()).nextBytes(data);

        numberOfLedgers = Integer.parseInt(props.getProperty("numLedgers", "25"));
        maxSizeBytes = Long.parseLong(props.getProperty("maxLedgerSizeMB", "2048")) * 1024 * 1024;
        maxEntries = maxSizeBytes / entrySize;

        proxy = new BKProxyClient(props.getProperty("proxyAddress", "localhost"), 
                Integer.parseInt(props.getProperty("proxyPort", "5555")));
        proxy.init();

        writeLatency = metrics.timer(buildMetricName("writeLatency"));
        bytesRate = metrics.meter(buildMetricName("bytesRate"));
    }

    @Override
    public void tearDown() {
        try {
            proxy.close();
        } catch (Exception e) {
            LOG.error("unexpected exception on proxy.close()", e);
        }
    }

    @Override
    public boolean next() throws Exception {
        if(extId == null) {
            final CreateExtentReturnValue rv = proxy.createExtent();
            if (rv.getReturnCode() != BKPConstants.SF_OK) {
                LOG.error("failed to create extent: {}", rv.getReturnCode());
                return false;
            }
            extId = rv.getExtentId();
            written = 0;
            // in proxy fragment ids start at 1
            fragmentId = 1;
            ledgers++;
        }
        
        final Timer.Context ct = writeLatency.time();
        try {
            proxy.writeFragment(extId, fragmentId, data);
        } finally {
            ct.stop();
            fragmentId++;
        }
        bytesRate.mark(data.length);
        written += data.length;
        
        if(written > maxSizeBytes - entrySize) {
            proxy.writeCloseExtent(extId);
            extId = null;
        }
        
        return ledgers < numberOfLedgers;
    }

}
