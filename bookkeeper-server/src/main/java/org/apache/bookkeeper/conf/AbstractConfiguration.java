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

import static org.apache.bookkeeper.conf.ClientConfiguration.CLIENT_AUTH_PROVIDER_FACTORY_CLASS;

import java.net.URL;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.net.ssl.SSLEngine;

import org.apache.bookkeeper.feature.Feature;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.util.EntryFormatter;
import org.apache.bookkeeper.util.JsonUtil;
import org.apache.bookkeeper.util.JsonUtil.ParseJsonException;
import org.apache.bookkeeper.util.LedgerIdFormatter;
import org.apache.bookkeeper.util.ReflectionUtils;
import org.apache.bookkeeper.util.StringEntryFormatter;
import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.SystemConfiguration;
import org.apache.commons.lang.StringUtils;

/**
 * Abstract configuration.
 */
public abstract class AbstractConfiguration<T extends AbstractConfiguration>
    extends CompositeConfiguration {

    public static final String READ_SYSTEM_PROPERTIES_PROPERTY = "org.apache.bookkeeper.conf.readsystemproperties";

    /**
     * Enable the use of System Properties, which was the default behaviour till 4.4.0.
     */
    private static final boolean READ_SYSTEM_PROPERTIES = Boolean.getBoolean(READ_SYSTEM_PROPERTIES_PROPERTY);

    protected static final ClassLoader DEFAULT_LOADER;
    static {
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        if (null == loader) {
            loader = AbstractConfiguration.class.getClassLoader();
        }
        DEFAULT_LOADER = loader;
    }

    // Zookeeper Parameters
    protected static final String ZK_TIMEOUT = "zkTimeout";
    protected static final String ZK_SERVERS = "zkServers";

    // Ledger Manager
    protected static final String LEDGER_MANAGER_TYPE = "ledgerManagerType";
    protected static final String LEDGER_MANAGER_FACTORY_CLASS = "ledgerManagerFactoryClass";
    protected static final String ZK_LEDGERS_ROOT_PATH = "zkLedgersRootPath";
    protected static final String ZK_REQUEST_RATE_LIMIT = "zkRequestRateLimit";
    protected static final String AVAILABLE_NODE = "available";
    protected static final String REREPLICATION_ENTRY_BATCH_SIZE = "rereplicationEntryBatchSize";

    // Metastore settings, only being used when LEDGER_MANAGER_FACTORY_CLASS is MSLedgerManagerFactory
    protected static final String METASTORE_IMPL_CLASS = "metastoreImplClass";
    protected static final String METASTORE_MAX_ENTRIES_PER_SCAN = "metastoreMaxEntriesPerScan";

    // Common TLS configuration
    // TLS Provider (JDK or OpenSSL)
    protected static final String TLS_PROVIDER = "tlsProvider";

    // TLS provider factory class name
    protected static final String TLS_PROVIDER_FACTORY_CLASS = "tlsProviderFactoryClass";

    protected static final String LEDGERID_FORMATTER_CLASS = "ledgerIdFormatterClass";
    protected static final String ENTRY_FORMATTER_CLASS = "entryFormatterClass";

    // Enable authentication of the other connection end point (mutual authentication)
    protected static final String TLS_CLIENT_AUTHENTICATION = "tlsClientAuthentication";

    /**
     * This list will be passed to {@link SSLEngine#setEnabledCipherSuites(java.lang.String[]) }.
     * Please refer to official JDK JavaDocs
    */
    protected static final String TLS_ENABLED_CIPHER_SUITES = "tlsEnabledCipherSuites";

    /**
     * This list will be passed to {@link SSLEngine#setEnabledProtocols(java.lang.String[]) }.
     * Please refer to official JDK JavaDocs
    */
    protected static final String TLS_ENABLED_PROTOCOLS = "tlsEnabledProtocols";

    /**
     * TLS KeyStore, TrustStore, Password files and Certificate Paths.
     */
    protected static final String TLS_KEYSTORE_TYPE = "tlsKeyStoreType";
    protected static final String TLS_KEYSTORE = "tlsKeyStore";
    protected static final String TLS_KEYSTORE_PASSWORD_PATH = "tlsKeyStorePasswordPath";
    protected static final String TLS_TRUSTSTORE_TYPE = "tlsTrustStoreType";
    protected static final String TLS_TRUSTSTORE = "tlsTrustStore";
    protected static final String TLS_TRUSTSTORE_PASSWORD_PATH = "tlsTrustStorePasswordPath";
    protected static final String TLS_CERTIFICATE_PATH = "tlsCertificatePath";

    //Netty configuration
    protected static final String NETTY_MAX_FRAME_SIZE = "nettyMaxFrameSizeBytes";
    protected static final int DEFAULT_NETTY_MAX_FRAME_SIZE = 5 * 1024 * 1024; // 5MB

    // Zookeeper ACL settings
    protected static final String ZK_ENABLE_SECURITY = "zkEnableSecurity";

    // Kluge for compatibility testing. Never set this outside tests.
    public static final String LEDGER_MANAGER_FACTORY_DISABLE_CLASS_CHECK = "ledgerManagerFactoryDisableClassCheck";

    // Validate bookie process user
    public static final String PERMITTED_STARTUP_USERS = "permittedStartupUsers";

    protected AbstractConfiguration() {
        super();
        if (READ_SYSTEM_PROPERTIES) {
            // add configuration for system properties
            addConfiguration(new SystemConfiguration());
        }
    }

    /**
     * Limit who can start the application to prevent future permission errors.
     */
    public void setPermittedStartupUsers(String s) {
        setProperty(PERMITTED_STARTUP_USERS, s);
    }

    /**
     * Get array of users specified in this property.
     */
    public String[] getPermittedStartupUsers() {
        return getStringArray(PERMITTED_STARTUP_USERS);
    }

    /**
     * You can load configurations in precedence order. The first one takes
     * precedence over any loaded later.
     *
     * @param confURL
     *          Configuration URL
     */
    @SuppressWarnings("unchecked")
    public void loadConf(URL confURL) throws ConfigurationException {
        PropertiesConfiguration loadedConf = new PropertiesConfiguration(confURL);
        for (Iterator<String> iter = loadedConf.getKeys(); iter.hasNext(); ) {
            String key = iter.next();
            setProperty(key, loadedConf.getProperty(key));
        }
    }

    /**
     * You can load configuration from other configuration.
     *
     * @param baseConf
     *          Other Configuration
     */
    @SuppressWarnings("unchecked")
    public void loadConf(CompositeConfiguration baseConf) {
        for (Iterator<String> iter = baseConf.getKeys(); iter.hasNext(); ) {
            String key = iter.next();
            setProperty(key, baseConf.getProperty(key));
        }
    }

    /**
     * Get zookeeper servers to connect.
     *
     * @return zookeeper servers
     */
    public String getZkServers() {
        List servers = getList(ZK_SERVERS, null);
        if (null == servers || 0 == servers.size()) {
            return null;
        }
        return StringUtils.join(servers, ",");
    }

    /**
     * Set zookeeper servers to connect.
     *
     * @param zkServers
     *          ZooKeeper servers to connect
     */
    public T setZkServers(String zkServers) {
        setProperty(ZK_SERVERS, zkServers);
        return getThis();
    }

    /**
     * Get zookeeper timeout.
     *
     * @return zookeeper server timeout
     */
    public int getZkTimeout() {
        return getInt(ZK_TIMEOUT, 10000);
    }

    /**
     * Set zookeeper timeout.
     *
     * @param zkTimeout
     *          ZooKeeper server timeout
     * @return server configuration
     */
    public T setZkTimeout(int zkTimeout) {
        setProperty(ZK_TIMEOUT, Integer.toString(zkTimeout));
        return getThis();
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
                                        DEFAULT_LOADER);
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
     * Get zookeeper access request rate limit.
     *
     * @return zookeeper access request rate limit.
     */
    public double getZkRequestRateLimit() {
        return getDouble(ZK_REQUEST_RATE_LIMIT, 0);
    }

    /**
     * Set zookeeper access request rate limit.
     *
     * @param rateLimit
     *          zookeeper access request rate limit.
     */
    public void setZkRequestRateLimit(double rateLimit) {
        setProperty(ZK_REQUEST_RATE_LIMIT, rateLimit);
    }

    /**
     * Are z-node created with strict ACLs.
     *
     * @return usage of secure ZooKeeper ACLs
     */
    public boolean isZkEnableSecurity() {
        return getBoolean(ZK_ENABLE_SECURITY, false);
    }

    /**
     * Set the usage of ACLs of new z-nodes.
     *
     * @param zkEnableSecurity
     */
    public void setZkEnableSecurity(boolean zkEnableSecurity) {
        setProperty(ZK_ENABLE_SECURITY, zkEnableSecurity);
    }

    /**
     * Get the node under which available bookies are stored.
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
     * Get the re-replication entry batch size.
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

    public void setFeature(String configProperty, Feature feature) {
        setProperty(configProperty, feature);
    }

    public Feature getFeature(String configProperty, Feature defaultValue) {
        if (null == getProperty(configProperty)) {
            return defaultValue;
        } else {
            return (Feature) getProperty(configProperty);
        }
    }

    /**
     * Set Ledger id formatter Class.
     *
     * @param formatterClass
     *          LedgerIdFormatter Class
     */
    public void setLedgerIdFormatterClass(Class<? extends LedgerIdFormatter> formatterClass) {
        setProperty(LEDGERID_FORMATTER_CLASS, formatterClass.getName());
    }

    /**
     * Get ledger id formatter class.
     *
     * @return LedgerIdFormatter class
     */
    public Class<? extends LedgerIdFormatter> getLedgerIdFormatterClass()
        throws ConfigurationException {
        return ReflectionUtils.getClass(this, LEDGERID_FORMATTER_CLASS,
                                        null, LedgerIdFormatter.UUIDLedgerIdFormatter.class,
                                        LedgerIdFormatter.class.getClassLoader());
    }

    /**
     * Set entry formatter Class.
     *
     * @param formatterClass
     *          EntryFormatter Class
     */
    public void setEntryFormatterClass(Class<? extends EntryFormatter> formatterClass) {
        setProperty(ENTRY_FORMATTER_CLASS, formatterClass.getName());
    }

    /**
     * Get entry formatter class.
     *
     * @return EntryFormatter class
     */
    public Class<? extends EntryFormatter> getEntryFormatterClass()
        throws ConfigurationException {
        return ReflectionUtils.getClass(this, ENTRY_FORMATTER_CLASS,
                                        null, StringEntryFormatter.class,
                                        EntryFormatter.class.getClassLoader());
    }

    /**
     * Set the client authentication provider factory class name.
     * If this is not set, no authentication will be used
     *
     * @param factoryClass
     *          the client authentication provider factory class name
     * @return client configuration
     */
    public T setClientAuthProviderFactoryClass(
            String factoryClass) {
        setProperty(CLIENT_AUTH_PROVIDER_FACTORY_CLASS, factoryClass);
        return getThis();
    }

    /**
     * Get the client authentication provider factory class name.
     * If this returns null, no authentication will take place.
     *
     * @return the client authentication provider factory class name or null.
     */
    public String getClientAuthProviderFactoryClass() {
        return getString(CLIENT_AUTH_PROVIDER_FACTORY_CLASS, null);
    }

    /**
     * Get the maximum netty frame size in bytes.  Any message received larger
     * that this will be rejected.
     *
     * @return the maximum netty frame size in bytes.
     */
    public int getNettyMaxFrameSizeBytes() {
        return getInt(NETTY_MAX_FRAME_SIZE, DEFAULT_NETTY_MAX_FRAME_SIZE);
    }

    /**
     * Set the max number of bytes a single message can be that is read by the bookie.
     * Any message larger than that size will be rejected.
     *
     * @param maxSize
     *          the max size in bytes
     * @return server configuration
     */
    public T setNettyMaxFrameSizeBytes(int maxSize) {
        setProperty(NETTY_MAX_FRAME_SIZE, String.valueOf(maxSize));
        return getThis();
    }

    /**
     * Get the security provider factory class name. If this returns null, no security will be enforced on the channel.
     *
     * @return the security provider factory class name or null.
     */
    public String getTLSProviderFactoryClass() {
        return getString(TLS_PROVIDER_FACTORY_CLASS, null);
    }

    /**
     * Set the client security provider factory class name. If this is not set, no security will be used on the channel.
     *
     * @param factoryClass
     *            the client security provider factory class name
     * @return client configuration
     */
    public T setTLSProviderFactoryClass(String factoryClass) {
        setProperty(TLS_PROVIDER_FACTORY_CLASS, factoryClass);
        return getThis();
    }

    /**
     * Get TLS Provider (JDK or OpenSSL).
     *
     * @return the TLS provider to use in creating TLS Context
     */
    public String getTLSProvider() {
        return getString(TLS_PROVIDER, "OpenSSL");
    }

    /**
     * Set TLS Provider (JDK or OpenSSL).
     *
     * @param provider
     *            TLS Provider type
     * @return Client Configuration
     */
    public T setTLSProvider(String provider) {
        setProperty(TLS_PROVIDER, provider);
        return getThis();
    }

    /**
     * Whether the client will send an TLS certificate on TLS-handshake.
     *
     * @see #setTLSAuthentication(boolean)
     * @return whether TLS is enabled on the bookie or not.
     */
    public boolean getTLSClientAuthentication() {
        return getBoolean(TLS_CLIENT_AUTHENTICATION, false);
    }

    /**
     * Specify whether the client will send an TLS certificate on TLS-handshake.
     *
     * @param enabled
     *            Whether to send a certificate or not
     * @return client configuration
     */
    public T setTLSClientAuthentication(boolean enabled) {
        setProperty(TLS_CLIENT_AUTHENTICATION, enabled);
        return getThis();
    }

    /**
     * Set the list of enabled TLS cipher suites. Leave null not to override default JDK list. This list will be passed
     * to {@link SSLEngine#setEnabledCipherSuites(java.lang.String[]) }. Please refer to official JDK JavaDocs
     *
     * @param list
     *            comma separated list of enabled TLS cipher suites
     * @return current configuration
     */
    public T setTLSEnabledCipherSuites(
            String list) {
        setProperty(TLS_ENABLED_CIPHER_SUITES, list);
        return getThis();
    }

    /**
     * Get the list of enabled TLS cipher suites.
     *
     * @return this list of enabled TLS cipher suites
     *
     * @see #setTLSEnabledCipherSuites(java.lang.String)
     */
    public String getTLSEnabledCipherSuites() {
        return getString(TLS_ENABLED_CIPHER_SUITES, null);
    }

    /**
     * Set the list of enabled TLS protocols. Leave null not to override default JDK list. This list will be passed to
     * {@link SSLEngine#setEnabledProtocols(java.lang.String[]) }. Please refer to official JDK JavaDocs
     *
     * @param list
     *            comma separated list of enabled TLS cipher suites
     * @return current configuration
     */
    public T setTLSEnabledProtocols(
            String list) {
        setProperty(TLS_ENABLED_PROTOCOLS, list);
        return getThis();
    }

    /**
     * Get the list of enabled TLS protocols.
     *
     * @return the list of enabled TLS protocols.
     *
     * @see #setTLSEnabledProtocols(java.lang.String)
     */
    public String getTLSEnabledProtocols() {
        return getString(TLS_ENABLED_PROTOCOLS, null);
    }


    /**
     * Trickery to allow inheritance with fluent style.
     */
    protected abstract T getThis();

    /**
     * returns the string representation of json format of this config.
     *
     * @return
     * @throws ParseJsonException
     */
    public String asJson() throws ParseJsonException {
        return JsonUtil.toJson(toMap());
    }

    private Map<String, Object> toMap() {
        Map<String, Object> configMap = new HashMap<>();
        Iterator<String> iterator = this.getKeys();
        while (iterator.hasNext()) {
            String key = iterator.next().toString();
            Object property = this.getProperty(key);
            if (property != null) {
                configMap.put(key, property.toString());
            }
        }
        return configMap;
    }
}
