/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bookkeeper.conf;

import java.net.URL;
import java.time.Clock;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.SystemConfiguration;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.util.ReflectionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Abstract configuration
 */
public abstract class AbstractConfiguration extends CompositeConfiguration {

    static final Logger LOG = LoggerFactory.getLogger(AbstractConfiguration.class);
    public static final String READ_SYSTEM_PROPERTIES_PROPERTY
                            = "org.apache.bookkeeper.conf.readsystemproperties";
    /**
     * Enable the use of System Properties, which was the default behaviour till 4.4.0
     */
    private static final boolean READ_SYSTEM_PROPERTIES
                                    = Boolean.getBoolean(READ_SYSTEM_PROPERTIES_PROPERTY);

    protected static final ClassLoader defaultLoader;
    static {
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        if (null == loader) {
            loader = AbstractConfiguration.class.getClassLoader();
        }
        defaultLoader = loader;
    }

    // Ledger Manager
    protected final static String LEDGER_MANAGER_TYPE = "ledgerManagerType";
    protected final static String LEDGER_MANAGER_FACTORY_CLASS = "ledgerManagerFactoryClass";
    protected final static String ZK_LEDGERS_ROOT_PATH = "zkLedgersRootPath";
    protected final static String AVAILABLE_NODE = "available";
    protected final static String REREPLICATION_ENTRY_BATCH_SIZE = "rereplicationEntryBatchSize";

    // Metastore settings, only being used when LEDGER_MANAGER_FACTORY_CLASS is MSLedgerManagerFactory
    protected final static String METASTORE_IMPL_CLASS = "metastoreImplClass";
    protected final static String METASTORE_MAX_ENTRIES_PER_SCAN = "metastoreMaxEntriesPerScan";

    private volatile Map<DayOfWeek, List<DailyRange>> dailyRanges = null;
    private final Object rangesLock = new Object(); 
    
    private final static String propForRangePattern = "%s@%s";
    private final static String timerangePattern = "timerange%d";
    private final static String timerangeStart = ".start";
    private final static String timerangeEnd = ".end";
    private final static String timerangeDays = ".days";
    
    private final Clock localClock = Clock.systemDefaultZone();

    protected AbstractConfiguration() {
        super();
        if (READ_SYSTEM_PROPERTIES) {
            // add configuration for system properties
            addConfiguration(new SystemConfiguration());
        }
    }

    // immutable
    final class DailyRange {
        public final String name;
        public final LocalTime start;
        public final LocalTime end;
        
        public DailyRange(String name, LocalTime start, LocalTime end) {
            if (start.isAfter(end)) {
                throw new IllegalArgumentException(
                        String.format("parsing range %s failed: start time %s cannot be after end time %s", name,
                                start.toString(), end.toString()));
            }

            this.start = start;
            this.end = end;
            this.name = name;
        }

        @Override
        public String toString() {
            return String.format("%s: from %s to %s", name, start.toString(), end.toString());
        }
    }
    
    public LocalTime getLocalTime() {
        return LocalTime.from(localClock.instant().atZone(ZoneId.systemDefault())).truncatedTo(ChronoUnit.MINUTES);
    }

    public DayOfWeek getDayOfWeek() {
        return LocalDate.from(localClock.instant().atZone(ZoneId.systemDefault())).getDayOfWeek();
    }

    protected Map<DayOfWeek, List<DailyRange>> getDailyRanges() {
        loadDailyRangesIfNeeded();
        return dailyRanges;
    }
    
    // helper to format available ranges
    protected static String rangesToString(Map<DayOfWeek, List<DailyRange>> ranges) {
        if (ranges == null) {
            return "null";
        }

        StringBuilder sb = new StringBuilder();
        sb.append(System.lineSeparator());
        for (DayOfWeek day : DayOfWeek.values()) {
            sb.append(day.toString()).append(":").append(System.lineSeparator());
            for (DailyRange dr : ranges.get(day)) {
                sb.append("    ");
                if (dr == null) {
                    sb.append("null");
                } else {
                    sb.append(dr.toString());
                }
                sb.append(System.lineSeparator());
            }
        }

        return sb.toString();
    }

    private void loadDailyRangesIfNeeded() {
        if (dailyRanges == null) {
            synchronized (rangesLock) {
                if (dailyRanges == null) {
                    final Map<DayOfWeek, List<DailyRange>> ranges = new HashMap<>();
                    for (DayOfWeek dow : DayOfWeek.values()) {
                        ranges.put(dow, Lists.<DailyRange>newArrayList());
                    }

                    for (int i = 0; i < Integer.MAX_VALUE; ++i) {
                        final String rangeName = String.format(timerangePattern, i);
                        final String startKey = rangeName + timerangeStart;
                        final String endKey = rangeName + timerangeEnd;
                        if (!this.containsKey(startKey) || !this.containsKey(endKey)) {
                            break;
                        }
                        
                        try {
                            for (DayOfWeek day : parseDays(this.getStringArray(rangeName + timerangeDays))) {
                                LocalTime start = LocalTime.parse(this.getString(startKey));
                                LocalTime end = LocalTime.parse(this.getString(endKey));
                                ranges.get(day).add(new DailyRange(rangeName, start, end));
                             }
                        } catch(Exception e) {
                            LOG.error("failed to parse time range {}", rangeName, e);
                            break;
                        }
                    }
                    
                    // let's make it all immutable.
                    for (DayOfWeek dow : DayOfWeek.values()) {
                        ranges.put(dow, ImmutableList.copyOf(ranges.get(dow)));
                    }
                    dailyRanges = ImmutableMap.copyOf(ranges);
                }
                LOG.info("Configuration loaded folowing effective time ranges: " + rangesToString(dailyRanges));
            }
        }
    }
    
    private static Set<DayOfWeek> parseDays(String[] values) {
        final Set<DayOfWeek> days;
        if (values == null || values.length == 0) {
            days = Sets.newHashSet(DayOfWeek.values());
        } else {
            days = Sets.newHashSet();
            for (String s : values) {
                days.add(DayOfWeek.valueOf(s.trim().toUpperCase()));
            }
        }
        return days;
    }

    @VisibleForTesting
    protected List<String> getActiveDailyRanges(LocalTime now, DayOfWeek dayOfWeek) {
        return getActiveDailyRanges(getDailyRanges(), now, dayOfWeek);
    }

    private static List<String> getActiveDailyRanges(Map<DayOfWeek, List<DailyRange>> ranges, LocalTime now,
            DayOfWeek dayOfWeek) {
        ImmutableList.Builder<String> result = ImmutableList.builder(); 
        List<DailyRange> todaysRanges = ranges.get(dayOfWeek);
        for (DailyRange dr : todaysRanges) {
            // includes start time, excludes end time.
            if (now.compareTo(dr.start) >= 0 && now.isBefore(dr.end)) {
                result.add(dr.name);
            }
        }
        return result.build();
    }

    /*
     * a way to provide callback to iterate over all possible timeranges to
     * perform validation of parameters subset
     */
    public void validateParametersForAllTimeRanges(ConfigurationValidator validator) throws IllegalArgumentException {
        Map<DayOfWeek, List<DailyRange>> ranges = getDailyRanges();
        for (DayOfWeek day : ranges.keySet()) {
            for (DailyRange dr : ranges.get(day)) {
                try {
                    // period start time is always included in the time range
                    // the only problem is with overlapping ranges. The first
                    // one wins.
                    validator.validateAtTimeAndDay(dr.start, day);
                } catch (Throwable t) {
                    throw new IllegalArgumentException("validation failed for timerange " + dr.name, t);
                }
            }
        }
    }
    
    protected String getPropertyNameForFirstActiveTimerange(String propName) {
        return getPropertyNameForFirstActiveTimerange(propName, getLocalTime(), getDayOfWeek());
    }

    @VisibleForTesting
    protected String getPropertyNameForFirstActiveTimerange(String propName, LocalTime now, DayOfWeek dayOfWeek) {
        for (String range : getActiveDailyRanges(now, dayOfWeek)) {
            final String propForRange = String.format(propForRangePattern, propName, range);
            if (this.containsKey(propForRange) && this.getProperty(propForRange) != null) {
                return propForRange;
            }
        }

        return propName;
    }

    /**
     * You can load configurations in precedence order. The first one takes
     * precedence over any loaded later.
     *
     * @param confURL
     *          Configuration URL
     */
    public void loadConf(URL confURL) throws ConfigurationException {
        Configuration loadedConf = new PropertiesConfiguration(confURL);
        addConfiguration(loadedConf);
    }

    /**
     * You can load configuration from other configuration
     *
     * @param baseConf
     *          Other Configuration
     */
    public void loadConf(AbstractConfiguration baseConf) {
        addConfiguration(baseConf); 
    }

    /**
     * Load configuration from other configuration object
     *
     * @param otherConf
     *          Other configuration object
     */
    public void loadConf(Configuration otherConf) {
        addConfiguration(otherConf);
    }

    /**
     * Set Ledger Manager Type.
     *
     * @param lmType
     *          Ledger Manager Type
     * @deprecated replaced by {@link #setLedgerManagerFactoryClass}
     */
    @Deprecated
    public void setLedgerManagerType(String lmType) {
        setProperty(LEDGER_MANAGER_TYPE, lmType); 
    }

    /**
     * Get Ledger Manager Type.
     *
     * @return ledger manager type
     * @throws ConfigurationException
     * @deprecated replaced by {@link #getLedgerManagerFactoryClass()}
     */
    @Deprecated
    public String getLedgerManagerType() {
        return getString(LEDGER_MANAGER_TYPE);
    }

    /**
     * Set Ledger Manager Factory Class Name.
     *
     * @param factoryClassName
     *          Ledger Manager Factory Class Name
     */
    public void setLedgerManagerFactoryClassName(String factoryClassName) {
        setProperty(LEDGER_MANAGER_FACTORY_CLASS, factoryClassName);
    }

    /**
     * Set Ledger Manager Factory Class.
     *
     * @param factoryClass
     *          Ledger Manager Factory Class
     */
    public void setLedgerManagerFactoryClass(Class<? extends LedgerManagerFactory> factoryClass) {
        setProperty(LEDGER_MANAGER_FACTORY_CLASS, factoryClass.getName());
    }

    /**
     * Get ledger manager factory class.
     *
     * @return ledger manager factory class
     */
    public Class<? extends LedgerManagerFactory> getLedgerManagerFactoryClass()
        throws ConfigurationException {
        return ReflectionUtils.getClass(this, LEDGER_MANAGER_FACTORY_CLASS,
                                        null, LedgerManagerFactory.class,
                                        defaultLoader);
    }

    /**
     * Set Zk Ledgers Root Path.
     *
     * @param zkLedgersPath zk ledgers root path
     */
    public void setZkLedgersRootPath(String zkLedgersPath) {
        setProperty(ZK_LEDGERS_ROOT_PATH, zkLedgersPath);
    }

    /**
     * Get Zk Ledgers Root Path.
     *
     * @return zk ledgers root path
     */
    public String getZkLedgersRootPath() {
        return getString(ZK_LEDGERS_ROOT_PATH, "/ledgers");
    }

    /**
     * Get the node under which available bookies are stored
     *
     * @return Node under which available bookies are stored.
     */
    public String getZkAvailableBookiesPath() {
        return getZkLedgersRootPath() + "/" + AVAILABLE_NODE;
    }

    /**
     * Set the max entries to keep in fragment for re-replication. If fragment
     * has more entries than this count, then the original fragment will be
     * split into multiple small logical fragments by keeping max entries count
     * to rereplicationEntryBatchSize. So, re-replication will happen in batches
     * wise.
     */
    public void setRereplicationEntryBatchSize(long rereplicationEntryBatchSize) {
        setProperty(REREPLICATION_ENTRY_BATCH_SIZE, rereplicationEntryBatchSize);
    }

    /**
     * Get the re-replication entry batch size
     */
    public long getRereplicationEntryBatchSize() {
        return getLong(REREPLICATION_ENTRY_BATCH_SIZE, 10);
    }

    /**
     * Get metastore implementation class.
     *
     * @return metastore implementation class name.
     */
    public String getMetastoreImplClass() {
        return getString(METASTORE_IMPL_CLASS);
    }

    /**
     * Set metastore implementation class.
     *
     * @param metastoreImplClass
     *          Metastore implementation Class name.
     */
    public void setMetastoreImplClass(String metastoreImplClass) {
        setProperty(METASTORE_IMPL_CLASS, metastoreImplClass);
    }

    /**
     * Get max entries per scan in metastore.
     *
     * @return max entries per scan in metastore.
     */
    public int getMetastoreMaxEntriesPerScan() {
        return getInt(METASTORE_MAX_ENTRIES_PER_SCAN, 50);
    }

    /**
     * Set max entries per scan in metastore.
     *
     * @param maxEntries
     *          Max entries per scan in metastore.
     */
    public void setMetastoreMaxEntriesPerScan(int maxEntries) {
        setProperty(METASTORE_MAX_ENTRIES_PER_SCAN, maxEntries);
    }
}
