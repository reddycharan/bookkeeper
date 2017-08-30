package org.apache.bookkeeper.test;

import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.stats.OpStatsData;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stats.StatsProvider;
import org.apache.commons.configuration.Configuration;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

/**
 * Dead simple in-memory stat provider for use in unit tests
 */
public class TestStatsProvider implements StatsProvider {
    public class TestCounter implements Counter {
        private AtomicLong val = new AtomicLong(0);

        @Override
        public void clear() {
            val.set(0);
        }

        @Override
        public void inc() {
            val.incrementAndGet();
        }

        @Override
        public void dec() {
            val.decrementAndGet();
        }

        @Override
        public void add(long delta) {
            val.addAndGet(delta);
        }

        @Override
        public Long get() {
            return val.get();
        }
    }

    public class TestOpStatsLogger implements OpStatsLogger {
        private long successCount;
        private long successValue;

        private long failureCount;
        private long failureValue;

        TestOpStatsLogger() {
            clear();
        }

        @Override
        public void registerFailedEvent(long eventLatency, TimeUnit unit) {
            registerFailedValue(unit.convert(eventLatency, TimeUnit.NANOSECONDS));
        }

        @Override
        public void registerSuccessfulEvent(long eventLatency, TimeUnit unit) {
            registerSuccessfulValue(unit.convert(eventLatency, TimeUnit.NANOSECONDS));
        }

        @Override
        synchronized public void registerSuccessfulValue(long value) {
            successCount++;
            successValue += value;
        }

        @Override
        synchronized public void registerFailedValue(long value) {
            failureCount++;
            failureValue += value;
        }

        @Override
        public OpStatsData toOpStatsData() {
            // Not supported at this time
            return null;
        }

        @Override
        synchronized public void clear() {
            successCount = 0;
            successValue = 0;
            failureCount = 0;
            failureValue = 0;
        }

        synchronized public double getSuccessAverage() {
            if (successCount == 0)
                return 0;
            return successValue / (double) successCount;
        }

        synchronized public long getSuccessCount() {
            return successCount;
        }
    }

    public class TestStatsLogger implements StatsLogger {
        private String path;

        TestStatsLogger(String path) {
            this.path = path;
        }

        private String getSubPath(String name) {
            if (path.isEmpty()) {
                return name;
            } else {
                return path + "." + name;
            }
        }

        @Override
        public OpStatsLogger getOpStatsLogger(String name) {
            return TestStatsProvider.this.getOrCreateOpStatsLogger(getSubPath(name));
        }

        @Override
        public Counter getCounter(String name) {
            return TestStatsProvider.this.getOrCreateCounter(getSubPath(name));
        }

        @Override
        public <T extends Number> void registerGauge(String name, Gauge<T> gauge) {
            TestStatsProvider.this.registerGauge(getSubPath(name), gauge);
        }

        @Override
        public StatsLogger scope(String name) {
            return new TestStatsLogger(getSubPath(name));
        }
    }

    @Override
    public void start(Configuration conf) {
    }

    @Override
    public void stop() {
    }

    private Map<String, TestOpStatsLogger> opStatLoggerMap = new ConcurrentHashMap<>();
    private Map<String, TestCounter> counterMap = new ConcurrentHashMap<>();
    private Map<String, Gauge<? extends Number>> gaugeMap = new ConcurrentHashMap<>();

    @Override
    public StatsLogger getStatsLogger(String scope) {
        return new TestStatsLogger(scope);
    }

    public TestOpStatsLogger getOpStatsLogger(String path) {
        return opStatLoggerMap.get(path);
    }

    public TestCounter getCounter(String path) {
        return counterMap.get(path);
    }

    public Gauge<? extends Number> getGauge(String path) {
        return gaugeMap.get(path);
    }

    public void forEachOpStatLogger(BiConsumer<String, TestOpStatsLogger> f) {
        for (Map.Entry<String, TestOpStatsLogger> entry : opStatLoggerMap.entrySet()) {
            f.accept(entry.getKey(), entry.getValue());
        }
    }

    public void clear() {
        for (TestOpStatsLogger logger : opStatLoggerMap.values()) {
            logger.clear();
        }
        for (TestCounter counter : counterMap.values()) {
            counter.clear();
        }
    }

    private TestOpStatsLogger getOrCreateOpStatsLogger(String path) {
        return opStatLoggerMap.computeIfAbsent(
                path,
                (String) -> new TestOpStatsLogger());
    }

    private TestCounter getOrCreateCounter(String path) {
        return counterMap.computeIfAbsent(
                path,
                (String) -> new TestCounter());
    }

    private <T extends Number> void registerGauge(String name, Gauge<T> gauge) {
        gaugeMap.put(name, gauge);
    }
}
