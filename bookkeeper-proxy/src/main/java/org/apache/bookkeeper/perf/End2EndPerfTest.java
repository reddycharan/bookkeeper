package org.apache.bookkeeper.perf;

import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Counter;
import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.util.concurrent.RateLimiter;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/*
 * Perf test runner. Takes name of properties file as a parameter.
 * Props file defines number of threads and scenarios to execute.
 * 
 * Prop file example:
 * 
 * scenario1.name=blah
 * scenario1.class=org.apache.bookkeeper.perf.blah
 * scenario1.threads=5
 * scenario1.throttle=5000
 * scenario1.config.a1=blah
 * scenario1.config.a2=blah2
 */
public class End2EndPerfTest {
    static final Logger LOG = LoggerFactory.getLogger(End2EndPerfTest.class);

    static { //lazy hack
        org.apache.log4j.BasicConfigurator.configure();
        org.apache.log4j.Logger.getRootLogger().setLevel(org.apache.log4j.Level.WARN);
        org.apache.log4j.Logger.getLogger(SfdcGenerateLedger.class).setLevel(org.apache.log4j.Level.INFO);
    }
    
    public static abstract class TestScenario implements Runnable {
        final Properties props;
        final private RateLimiter throttler;
        final MetricRegistry metrics;
        final String scenarioName;

        private final Counter scenariosRunning;
        
        private final Timer scenarioLatency;

        abstract public void setUp() throws Exception;
        abstract public void tearDown();
        abstract public boolean next() throws Exception;

        
        public TestScenario(String scenarioName, Properties props, MetricRegistry metrics, RateLimiter throttler) {
            this.scenarioName = scenarioName;
            this.props = props;
            this.throttler = throttler;
            this.metrics = metrics;
            
            scenarioLatency = metrics.timer(buildMetricName("scenarioLatency"));
            scenariosRunning = metrics
                    .counter(MetricRegistry.name(End2EndPerfTest.class, "scenariosRunning"));
        }

        public String buildMetricName(String name) {
            return MetricRegistry.name(this.getClass(), scenarioName, name);
        }
        
        public void acquire() {
            if(throttler != null) {
                throttler.acquire();
            }
        }

        @Override
        final public void run() {
            try {
                setUp();
                scenariosRunning.inc();
            } catch (Exception e) {
                LOG.error("Unexpected exception in setUp of scenario {}", scenarioName, e);
                return;
            }
            
            try {
                while(true) {
                    acquire();
                    final Timer.Context ct = scenarioLatency.time();
                    try {
                        if ( !next() ) {
                            break;
                        }
                    } finally {
                        ct.stop();
                    }
                }
            } catch (Exception e) {
                LOG.error("Unexpected exception in test of scenario {}", scenarioName, e);
            } finally {
                scenariosRunning.dec();
                tearDown();
            }
        }
    }

    private static final MetricRegistry metrics = new MetricRegistry();
    private static final ConsoleReporter consoleReporter = ConsoleReporter.forRegistry(metrics)
            .convertRatesTo(TimeUnit.SECONDS).convertDurationsTo(TimeUnit.MILLISECONDS).build();
    private static final JmxReporter jmxReporter = JmxReporter.forRegistry(metrics).convertRatesTo(TimeUnit.SECONDS)
            .convertDurationsTo(TimeUnit.MILLISECONDS).build();

    private static final Counter executorsRunning = metrics
            .counter(MetricRegistry.name(End2EndPerfTest.class, "executorsRunning"));

    public static final String scenarioPrefixTemplate = "scenario%d";
    public static final String scenarioNameProp = "name";
    public static final String scenarioThreadProp = "threads";
    public static final String scenarioThrottleProp = "throttle";
    public static final String scenarioClassProp = "class";
    public static final String scenarioConfigPrefix = "config";
    
    private static final int MAX_SCENARIOS = 255;

    public static void main(String[] args) throws Exception {
        if (args.length < 1 || args[0] == null || args[0].isEmpty()) {
            final String msg = "path to the properties file is required";
            System.out.println(msg);
            LOG.error(msg);
            return;
        }

        final Properties props;
        try {
            props = loadProps(args);
        } catch (Exception e) {
            final String msg = "cannot load props from " + args[0];
            System.out.println(msg);
            LOG.error(msg);
            return;
        }

        consoleReporter.start(30, TimeUnit.SECONDS);
        jmxReporter.start();

        final List<ExecutorService> executors = new LinkedList<>();

        for (int i = 0; i < MAX_SCENARIOS; ++i) {
            trySubmitScenario(props, executors, i);
        }

        awaitCompletion(executors);

        consoleReporter.report();
        consoleReporter.stop();
        jmxReporter.stop();

    }

    private static boolean trySubmitScenario(final Properties props, final List<ExecutorService> executors,
            int scenarioNum) throws Exception {
        final String scenarioId = String.format(scenarioPrefixTemplate, scenarioNum);
        final String className = props.getProperty(scenarioId + "." + scenarioClassProp, "");
        final String scenarioName = props.getProperty(scenarioId + "." + scenarioNameProp, "");
        if (scenarioName.isEmpty() || className.isEmpty()) {
            return false;
        }
        final int threadNum = Integer.parseInt(props.getProperty(scenarioId + "." + scenarioThreadProp, "1"));
        
        if (threadNum < 1) {
            return false;
        }

        ExecutorService exec = Executors.newFixedThreadPool(threadNum,
                new ThreadFactoryBuilder().setNameFormat(scenarioName + "-%d").build());
        executors.add(exec);

        final int maxQps = Integer.parseInt(props.getProperty(scenarioId + "." + scenarioThrottleProp, "0"));
        final RateLimiter throttler = (maxQps < 1) ? null : RateLimiter.create(maxQps);

        final String prefix = scenarioId + "." + scenarioConfigPrefix + ".";
        final Properties scenarioProps = filterProps(props, prefix);

        final Class<?> c = Class.forName(className);
        final Constructor<?> cons = c.getConstructor(String.class, Properties.class, MetricRegistry.class, RateLimiter.class);

        for (int j = 0; j < threadNum; ++j) {
            final TestScenario scenario = (TestScenario) cons.newInstance(scenarioName, scenarioProps, metrics, throttler);
            exec.submit(scenario);
        }

        executorsRunning.inc();
        return true;
    }

    private static Properties filterProps(Properties props, String prefix) {
        // removing prefix from matching property names
        final Properties out = new Properties();
        for (String propName: props.stringPropertyNames()) {
            if(propName.startsWith(prefix)) {
                final String name = propName.substring(prefix.length());
                out.setProperty(name, props.getProperty(propName));
            }
        }
        return out;
    }

    private static void awaitCompletion(final List<ExecutorService> executors) {
        for (ExecutorService es : executors) {
            es.shutdown();
            while (true) {
                try {
                    if (es.awaitTermination(10, TimeUnit.SECONDS)) {
                        break;
                    }
                } catch (InterruptedException e) {
                    // noop
                }
            }
            executorsRunning.dec();
        }
    }

    private static Properties loadProps(String[] args) throws IOException {
        final Properties props = new Properties();
        FileInputStream out = null;
        try {
            out = new FileInputStream(args[0]);
            props.load(out);
        } finally {
            if (out != null) {
                out.close();
            }
        }
        return props;
    }

}
