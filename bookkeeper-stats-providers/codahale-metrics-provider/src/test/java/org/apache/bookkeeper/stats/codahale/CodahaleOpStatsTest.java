/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.bookkeeper.stats.codahale;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.ServerSocket;
import javax.net.ssl.SSLContext;
import org.apache.bookkeeper.common.tls.JettyTlsContextFactoryBuilder;
import org.apache.bookkeeper.common.tls.TLSUtils;
import org.apache.bookkeeper.stats.OpStatsData;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.BasicHttpClientConnectionManager;
import org.junit.Test;

/**
 * Unit test of {@link CodahaleOpStatsLogger}.
 */

public class CodahaleOpStatsTest {
    private static final String CONFIG_JETTY_PORT = "jettyPort";
    private static final String CONFIG_JETTY_TLS = "jettyTLS";

    private static final String CONFIG_ENABLE_STATISTICS = "enableStatistics";
    private static final String CONFIG_STATSERVER_CONTEXT_PATH = "statServletContextPath";
    private static final String CONFIG_STATSERVER_ENDPOINT = "statServletEndpoint";
    private static final String CONFIG_ENABLE_REST_ENDPOINT = "enableRestEndpoints";
    private static final String CONFIG_REST_PACKAGE = "restPackage";
    private static final String CONFIG_REST_SERVLET_CONTEXT_PATH = "restServletContextPath";

    @Test
    public void testToOpStatsData() {
        OpStatsLogger logger = new CodahaleMetricsProvider().getStatsLogger("test").getOpStatsLogger("testLogger");
        logger.registerSuccessfulValue(1);
        // the following should not throw any exception
        OpStatsData statsData = logger.toOpStatsData();
        assertEquals(1, statsData.getNumSuccessfulEvents());
    }

    @Test
    public void testToFastOpStatsData() {
        OpStatsLogger logger = new FastCodahaleMetricsProvider().getStatsLogger("test").getOpStatsLogger("testLogger");
        logger.registerSuccessfulValue(1);
        // the following should not throw any exception
        OpStatsData statsData = logger.toOpStatsData();
        assertEquals(1, statsData.getNumSuccessfulEvents());
    }

    private String getResourcePath(String resource) throws Exception {
        return this.getClass().getClassLoader().getResource(resource).toURI().getPath();
    }

    private synchronized int findFreePort() {
        ServerSocket socket;
        try {
            socket = new ServerSocket(0);
            socket.setReuseAddress(true);
            int port = socket.getLocalPort();

            socket.close();
            //Give it some time to truly close the connection
            Thread.sleep(100);

            return port;
        } catch (IOException | InterruptedException e) {
            // Ignore exceptions
        }
        throw new IllegalStateException("could not find a free TCP/IP port");
    }

    private HttpResponse sendGetRequest(HttpClient client, String uri) throws IOException {
        return client.execute(new HttpGet(uri));
    }

    @Test
    public void testJettySSLPort() throws Exception {
        CodahaleMetricsProvider metricsProvider = new CodahaleMetricsProvider();
        Configuration conf = new BaseConfiguration();

        final int jettyServerPort = findFreePort();
        conf.addProperty(CONFIG_JETTY_PORT, String.valueOf(jettyServerPort));
        conf.addProperty(CONFIG_JETTY_TLS, "true");

        conf.addProperty(CONFIG_ENABLE_STATISTICS, true);
        conf.addProperty(CONFIG_STATSERVER_CONTEXT_PATH, "/stats");
        conf.addProperty(CONFIG_STATSERVER_ENDPOINT, "/metrics.json");
        conf.addProperty(CONFIG_ENABLE_REST_ENDPOINT, true);
        conf.addProperty(CONFIG_REST_PACKAGE, "org.apache.bookkeeper.util");
        conf.addProperty(CONFIG_REST_SERVLET_CONTEXT_PATH, "/rest");

        // tls configurations for PEM format keys
        conf.addProperty(TLSUtils.CONFIG_TLS_KEYSTORE_TYPE, "PEM");
        conf.addProperty(TLSUtils.CONFIG_TLS_KEYSTORE_PATH, getResourcePath("server-key.pem"));
        conf.addProperty(TLSUtils.CONFIG_TLS_TRUSTSTORE_TYPE, "PEM");
        conf.addProperty(TLSUtils.CONFIG_TLS_TRUSTSTORE_PATH, getResourcePath("client-cert.pem"));
        conf.addProperty(TLSUtils.CONFIG_TLS_CERTIFICATE_PATH, getResourcePath("server-cert.pem"));
        conf.addProperty(TLSUtils.CONFIG_TLS_CLIENT_AUTHENTICATION, true);

        metricsProvider.start(conf);

        // spin up a http client and query REST endpoint
        HttpClientBuilder builder = HttpClientBuilder.create();
        SSLContext sslContext = new JettyTlsContextFactoryBuilder()
                .setKeyStoreType("PEM")
                .setKeyStorePath(getResourcePath("client-key.pem"))
                .setKeyStorePasswordPath(null)
                .setTrustStoreType("PEM")
                .setTrustStorePath(getResourcePath("server-cert.pem"))
                .setTrustStorePasswordPath(null)
                .setCertificatePath(getResourcePath("client-cert.pem"))
                .setClientAuthentication(true)
                .buildSslContext();

        builder.setSSLContext(sslContext);

        SSLConnectionSocketFactory sslSocketFactory = new SSLConnectionSocketFactory(sslContext);
        PlainConnectionSocketFactory plainConnectionSocketFactory = new PlainConnectionSocketFactory();
        Registry<ConnectionSocketFactory> registry = RegistryBuilder.<ConnectionSocketFactory>create()
                                                        .register("https", sslSocketFactory)
                                                        .register("http", plainConnectionSocketFactory)
                                                        .build();

        HttpClientConnectionManager ccm = new BasicHttpClientConnectionManager(registry);
        builder.setConnectionManager(ccm);
        HttpClient httpClient = builder.build();

        // http get request should fail with http
        try {
            sendGetRequest(httpClient, "http://localhost:" + conf.getString(CONFIG_JETTY_PORT)
                    + conf.getString(CONFIG_STATSERVER_CONTEXT_PATH) + conf.getString(CONFIG_STATSERVER_ENDPOINT));
            fail("Unsecured connection to REST endpoint should have failed");
        } catch (Exception e) {
            // expected
        }

        // https request and response
        HttpResponse response = sendGetRequest(httpClient, "https://localhost:" + conf.getString(CONFIG_JETTY_PORT)
                + conf.getString(CONFIG_STATSERVER_CONTEXT_PATH) + conf.getString(CONFIG_STATSERVER_ENDPOINT));
        assertEquals(response.getStatusLine().getStatusCode(), org.apache.http.HttpStatus.SC_OK);
    }

    @Test
    public void testJettyNoSSL() throws Exception {
        CodahaleMetricsProvider metricsProvider = new CodahaleMetricsProvider();
        Configuration conf = new BaseConfiguration();

        final int jettyServerPort = findFreePort();
        conf.addProperty(CONFIG_JETTY_PORT, String.valueOf(jettyServerPort));
        conf.addProperty(CONFIG_JETTY_TLS, "false");

        conf.addProperty(CONFIG_ENABLE_STATISTICS, true);
        conf.addProperty(CONFIG_STATSERVER_CONTEXT_PATH, "/stats");
        conf.addProperty(CONFIG_STATSERVER_ENDPOINT, "/metrics.json");
        conf.addProperty(CONFIG_ENABLE_REST_ENDPOINT, true);
        conf.addProperty(CONFIG_REST_PACKAGE, "org.apache.bookkeeper.util");
        conf.addProperty(CONFIG_REST_SERVLET_CONTEXT_PATH, "/rest");

        metricsProvider.start(conf);

        // spin up a http client and query REST endpoint
        HttpClientBuilder builder = HttpClientBuilder.create();
        ConnectionSocketFactory socketFactory = PlainConnectionSocketFactory.getSocketFactory();
        Registry<ConnectionSocketFactory> registry = RegistryBuilder.<ConnectionSocketFactory>create()
                .register("http", socketFactory)
                .build();

        HttpClientConnectionManager ccm = new BasicHttpClientConnectionManager(registry);
        builder.setConnectionManager(ccm);
        HttpClient httpClient = builder.build();

        /* https request and response */

        HttpResponse response = sendGetRequest(httpClient, "http://localhost:" + conf.getString(CONFIG_JETTY_PORT)
                + conf.getString(CONFIG_STATSERVER_CONTEXT_PATH) + conf.getString(CONFIG_STATSERVER_ENDPOINT));
        assertEquals(response.getStatusLine().getStatusCode(), org.apache.http.HttpStatus.SC_OK);
    }
}
