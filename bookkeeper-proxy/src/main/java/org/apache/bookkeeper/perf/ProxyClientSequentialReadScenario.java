package org.apache.bookkeeper.perf;

import java.io.IOException;
import java.security.SecureRandom;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.apache.bookkeeper.BKPConstants;
import org.apache.bookkeeper.perf.End2EndPerfTest.TestScenario;
import org.apache.bookkeeper.proxyclient.BKProxyClient;
import org.apache.bookkeeper.proxyclient.ReturnValue.ExtentListGetReturnValue;
import org.apache.bookkeeper.proxyclient.ReturnValue.ReadFragmentReturnValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.util.concurrent.RateLimiter;

/*
 * Test scenario that simulates sequential read of ledgers via proxy.
 * It picks random ledger and reads it from first to the last entry. 
 */
public class ProxyClientSequentialReadScenario extends TestScenario {
    static final Logger LOG = LoggerFactory.getLogger(ProxyClientSequentialReadScenario.class);
    
    public ProxyClientSequentialReadScenario(String scenarioName, Properties props, MetricRegistry metrics,
            RateLimiter throttler) {
        super(scenarioName, props, metrics, throttler);
    }

    final Random random = new SecureRandom();
    
    int numberOfLedgers;

    BKProxyClient proxy;
    
    byte[] extId = null;
    int fragmentId = 0;

    Timer readLatency;
    Meter bytesRate;

    @Override
    public void setUp() throws Exception {
        numberOfLedgers = Integer.parseInt(props.getProperty("numLedgers", "25"));

        proxy = new BKProxyClient(props.getProperty("proxyAddress", "localhost"), 
                Integer.parseInt(props.getProperty("proxyPort", "5555")));
        proxy.init();

        readLatency = metrics.timer(buildMetricName("readLatency"));
        bytesRate = metrics.meter(buildMetricName("bytesRate"));
    }

    @Override
    public void tearDown() {
        try {
            proxy.close();
        } catch (Exception e) {
            LOG.error("unexpected exception on bk.close()", e);
        }
    }

    @Override
    public boolean next() throws Exception {
        if (extId == null) {
            numberOfLedgers--;
            if(numberOfLedgers < 0) {
                return false;
            }
            
            extId = selectNextLedger();
            if(extId == null) {
                LOG.warn("no ledger to read from, exiting");
                return false;
            }
            // in proxy fragment ids start at 1
            fragmentId = 1;
        }

        final Timer.Context ct = readLatency.time();
        try {
            ReadFragmentReturnValue rv = proxy.readFragment(extId, fragmentId);
            if (rv.getReturnCode() != BKPConstants.SF_OK) {
                LOG.warn(rv.getReturnCode() + 
                        ": fragment read failed, probably reached end of the ledger for ledger {} fragment {}, code: ", 
                        extId, fragmentId);
                extId = null;
                return true;
            }
            byte[] data = rv.getFragmentData();
            bytesRate.mark(data.length);
        } catch (Exception e) {
            LOG.warn("Exception while reading entry, probably reached end of the ledger", e);
            extId = null;
        } finally {
            ct.stop();
            fragmentId++;
        }
        return true;
    }

    private byte[] selectNextLedger() throws IOException {
        ExtentListGetReturnValue extentListGetReturnValue = proxy.getExtentsList();
        List<byte[]> extents = extentListGetReturnValue.getExtentIdsList();
        
        if(extents.size() < 1) {
            return null;
        }
        return extents.get(random.nextInt(extents.size()));
    }

}
