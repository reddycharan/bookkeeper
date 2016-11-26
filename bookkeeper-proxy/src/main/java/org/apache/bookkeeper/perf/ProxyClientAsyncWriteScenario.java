package org.apache.bookkeeper.perf;

import java.io.IOException;
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
 * Test scenario that simulates async writes via BK Proxy.
 * Scenario submits N async writes and then waits for the last one to complete.
 */
public class ProxyClientAsyncWriteScenario extends TestScenario {
    private static final int asyncWaitTimeoutMillis = 60_000;
    static final Logger LOG = LoggerFactory.getLogger(ProxyClientAsyncWriteScenario.class);
    
    public ProxyClientAsyncWriteScenario(String scenarioName, Properties props, MetricRegistry metrics,
            RateLimiter throttler) {
        super(scenarioName, props, metrics, throttler);
    }

    int entrySize;
    byte[] data;

    int numberOfLedgers;
    long maxSizeBytes;
    long maxEntries;
    long maxAsyncPending;
    long asyncPending = 0;

    BKProxyClient proxy;
    
    byte[] extId = null;
    int fragmentId = 0;
    long written =  0;
    long ledgers = 0;
    
    long delayBeforeWriteMicros = 0;
    long delayBeforeWriteMillis = 0;
    int delayBeforeWriteExtraNanos = 0;
    boolean doBusyWait;

    Timer asyncWriteSubmitLatency;
    Timer waitStatusLatency;
    Timer overallWriteLatency;
    Timer actualDelayBeforeWriteLatency;
    Meter bytesRate;

    @Override
    public void setUp() throws Exception {
        entrySize = Integer.parseInt(props.getProperty("entrySize", "65536"));
        data = new byte[entrySize];
        (new SecureRandom()).nextBytes(data);

        numberOfLedgers = Integer.parseInt(props.getProperty("numLedgers", "25"));
        maxSizeBytes = Long.parseLong(props.getProperty("maxLedgerSizeMB", "2048")) * 1024 * 1024;
        maxEntries = maxSizeBytes / entrySize;
        maxAsyncPending = Long.parseLong(props.getProperty("maxAsyncPending", "20"));
        
        delayBeforeWriteMicros = Long.parseLong(props.getProperty("delayBeforeWriteMicros", "0"));
        delayBeforeWriteMillis = delayBeforeWriteMicros / 1000;
        delayBeforeWriteExtraNanos = (int) ((delayBeforeWriteMicros % 1000) * 1000);
        doBusyWait = Boolean.parseBoolean(props.getProperty("doBusyWait", "true"));

        proxy = new BKProxyClient(props.getProperty("proxyAddress", "localhost"), 
                Integer.parseInt(props.getProperty("proxyPort", "5555")));
        proxy.init();

        actualDelayBeforeWriteLatency = metrics.timer(buildMetricName("actualDelayBeforeWriteLatency"));
        asyncWriteSubmitLatency = metrics.timer(buildMetricName("asyncWriteSubmitLatency"));
        waitStatusLatency = metrics.timer(buildMetricName("waitStatusLatency"));
        overallWriteLatency = metrics.timer(buildMetricName("overallWriteLatency"));
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
            asyncPending = 0;
            // in proxy fragment ids start at 1
            fragmentId = 1;
            ledgers++;
        }
        final Timer.Context ctOverall = overallWriteLatency.time();

        try {
            sleepIfNeeded();
            
            final Timer.Context ct = asyncWriteSubmitLatency.time();
            try {
                proxy.asyncWriteFragment(extId, fragmentId, data);
                asyncPending++;
            } finally {
                ct.stop();
                fragmentId++;
            }
            bytesRate.mark(data.length);
            written += data.length;
            
            if (asyncPending >= maxAsyncPending) {
                waitForBatchToComplete(extId, fragmentId);
                asyncPending = 0;
            }
            
            if(written > maxSizeBytes - entrySize && asyncPending > 0) {
                waitForBatchToComplete(extId, fragmentId);
                asyncPending = 0;
            }
        } finally {
            ctOverall.stop();
        }
        
        if(written > maxSizeBytes - entrySize) {
            proxy.writeCloseExtent(extId);
            extId = null;
        }
        
        return ledgers < numberOfLedgers;
    }

    private void sleepIfNeeded() throws InterruptedException {
        if(delayBeforeWriteMicros > 0) {
            final Timer.Context ct = actualDelayBeforeWriteLatency.time(); 
            try {
                if (doBusyWait) {
                    //let's do busy wait :(
                    // not a good idea for tests with many threads
                    final long sleepUntil = System.nanoTime() + delayBeforeWriteMicros * 1000;
                    while(System.nanoTime() < sleepUntil) {
                        // noop
                    }
                } else {
                    // below is not good for sub-millisecond sleeps ends up sleeping 1ms or more.
                    Thread.sleep(delayBeforeWriteMillis, delayBeforeWriteExtraNanos);
                }
            } finally {
                ct.stop();
            }
        }
    }

    private void waitForBatchToComplete(byte[] extentId, int fragmentId) {
        final Timer.Context ct = waitStatusLatency.time();
        try {
            proxy.asyncWriteStatus(extentId, fragmentId, asyncWaitTimeoutMillis);
        } catch (IOException e) {
            LOG.error("unexpected error in asyncWriteStatus", e);
        } finally {
            ct.stop();
        }
    }

}
