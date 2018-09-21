/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.bookkeeper.stats.codahale;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.servlets.MetricsServlet;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.common.tls.JettyTlsContextFactoryBuilder;
import org.apache.bookkeeper.common.tls.TLSUtils;
import org.apache.commons.configuration.Configuration;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides rest endpoints.
 */
public class JettyServices {

    private static final Logger LOG = LoggerFactory.getLogger(JettyServices.class);

    private Server jettyServer;
    private Configuration conf;
    private org.apache.bookkeeper.stats.codahale.CodahaleMetricsProvider statsProvider;
    private int port;
    private boolean statsEnabled;
    private boolean restEnabled;
    private boolean statsRunning;
    private boolean restRunning;
    private HandlerCollection handlerCollection;

    public JettyServices(
            Configuration conf,
            org.apache.bookkeeper.stats.codahale.CodahaleMetricsProvider statsProvider) throws Exception {
        this.conf = conf;
        this.statsRunning = false;
        this.restRunning = false;
        this.port = conf.getInt("jettyPort");
        this.statsEnabled = conf.getBoolean("enableStatistics");
        this.restEnabled = conf.getBoolean("enableRestEndpoints");
        this.statsProvider = statsProvider;
        if ((statsEnabled || restEnabled) && port == 0) {
            throw new Exception("REST and/or Stats enabled, but port is zero.");
        } else if ((statsEnabled || restEnabled)) {
            jettyServer = new Server();
            // True arg identifies this as "MutableWhenRunning" allowing us to
            // append handlers at runtime
            handlerCollection = new HandlerCollection(true);
        }
    }

    /*

     * Configure rest endpoints. True if success; else, false.
     */
    public void enableRestEndpoints() throws Exception {
        if (!restRunning && restEnabled) {
            ServletContextHandler context = new ServletContextHandler();
            context.setContextPath(conf.getString("restServletContextPath"));
            ResourceConfig config = new ResourceConfig();
            config.packages(conf.getString("restPackage"));
            ServletHolder servletHolder = new ServletHolder(new ServletContainer(config));
            context.addServlet(servletHolder, "/*");
            // Append our handler to any existing handlers
            appendHandler(context);
            restRunning = true;
        }
    }

    public void enableMetricEndpoint() throws Exception {
        if (!statsRunning && statsEnabled) {
            ServletContextHandler context = new ServletContextHandler();
            String contextPath = conf.getString("statServletContextPath", "/stats");
            String endpoint = conf.getString("statServletEndpoint", "/metrics.json");
            context.setContextPath(contextPath);
            context.setAttribute("show-jvm-metrics", "true");
            context.addServlet(new ServletHolder(new MetricsServlet()), endpoint);
            context.addEventListener(new MetricsServlet.ContextListener() {

                @Override
                protected MetricRegistry getMetricRegistry() {
                    return statsProvider.getMetrics();
                }

                @Override
                protected TimeUnit getRateUnit() {
                    return TimeUnit.SECONDS;
                }

                @Override
                protected TimeUnit getDurationUnit() {
                    return TimeUnit.MILLISECONDS;
                }
            });
            appendHandler(context);
            statsRunning = true;
        }
    }

    /*
     * Append handlers to the collection. Is permitted during runtime.
     */
    private void appendHandler(ServletContextHandler newHandler) throws Exception {
        handlerCollection.addHandler(newHandler);
        try {
            newHandler.start();
        } catch (Exception e) {
            LOG.error("Error starting new handler: ", e);
            throw e;
        }
    }

    /*
     * Start the server. Ensure it is not null (was able to be started) & not
     * running
     */
    public void start() throws Exception {
        if (jettyServer != null && !jettyServer.isRunning()) {

            // secure connections to jetty with TLS
            if (conf.getBoolean("jettyTLS", false)) {
                SslContextFactory sslContextFactory = new JettyTlsContextFactoryBuilder()
                        .setKeyStoreType(conf.getString(TLSUtils.CONFIG_TLS_KEYSTORE_TYPE, "PEM"))
                        .setKeyStorePath(conf.getString(TLSUtils.CONFIG_TLS_KEYSTORE_PATH, null))
                        .setKeyStorePasswordPath(conf.getString(TLSUtils.CONFIG_TLS_KEYSTORE_PASSWORD_PATH, null))
                        .setTrustStoreType(conf.getString(TLSUtils.CONFIG_TLS_KEYSTORE_TYPE, "PEM"))
                        .setTrustStorePath(conf.getString(TLSUtils.CONFIG_TLS_TRUSTSTORE_PATH, null))
                        .setTrustStorePasswordPath(conf.getString(TLSUtils.CONFIG_TLS_TRUSTSTORE_PASSWORD_PATH, null))
                        .setCertificatePath(conf.getString(TLSUtils.CONFIG_TLS_CERTIFICATE_PATH, null))
                        .setClientAuthentication(conf.getBoolean(TLSUtils.CONFIG_TLS_CLIENT_AUTHENTICATION, true))
                        .build();

                HttpConfiguration http = new HttpConfiguration();
                http.setSecureScheme("https");
                http.setSecurePort(this.port);

                HttpConfiguration https = new HttpConfiguration(http);
                https.addCustomizer(new SecureRequestCustomizer());

                ServerConnector sslConnector = new ServerConnector(jettyServer,
                        new SslConnectionFactory(sslContextFactory, "http/1.1"),
                        new HttpConnectionFactory(https));

                sslConnector.setPort(this.port);
                sslConnector.setIdleTimeout(50000);
                jettyServer.setConnectors(new ServerConnector[] { sslConnector });
            } else {
                ServerConnector serverConnector = new ServerConnector(jettyServer);
                serverConnector.setPort(this.port);
                jettyServer.setConnectors(new ServerConnector[] { serverConnector });
            }

            jettyServer.setHandler(handlerCollection);
            jettyServer.start();
            enableMetricEndpoint();
            enableRestEndpoints();
        }
    }

    /*
     * Shut down the server. Ensure it is not null (was able to be started)
     */
    public void shutDown() {
        if (jettyServer != null) {
            for (Handler sch : jettyServer.getHandlers()) {
                try {
                    sch.stop();
                } catch (Exception e) {
                    LOG.error("Error stopping jetty handler", e);
                }
            }
            try {
                jettyServer.stop();
            } catch (Exception e) {
                LOG.error("Exception stopping Jetty Services server: ", e);
            }
            jettyServer.destroy();
        }
    }

}
