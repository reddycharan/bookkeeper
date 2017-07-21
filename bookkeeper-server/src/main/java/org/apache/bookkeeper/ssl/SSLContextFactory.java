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
package org.apache.bookkeeper.ssl;

import org.apache.bookkeeper.conf.AbstractConfiguration;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;

import io.netty.buffer.PooledByteBufAllocator;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.OpenSsl;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.SslProvider;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.security.spec.InvalidKeySpecException;
import java.util.Arrays;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;

public class SSLContextFactory implements SecurityHandlerFactory {
    public enum KeyFileType {
        PKCS12, JKS, PEM;
    }

    private final static Logger LOG = LoggerFactory.getLogger(SSLContextFactory.class);
    private final static String SSLCONTEXT_HANDLER_NAME = "ssl";
    private String[] protocols;
    private String[] ciphers;
    private SslContext sslContext;

    private String getPasswordFromFile(String path) throws IOException {
        FileInputStream pwdin = new FileInputStream(path);
        byte[] pwd;
        try {
            File passwdFile = new File(path);
            if (passwdFile.length() == 0) {
                return "";
            }
            pwd = FileUtils.readFileToByteArray(passwdFile);
        } finally {
            pwdin.close();
        }
        return new String(pwd, "UTF-8");
    }

    private KeyStore loadKeyFile(String keyFileType, String keyFileLocation, String keyFilePassword)
            throws KeyStoreException, NoSuchAlgorithmException, CertificateException, IOException {
        KeyStore ks = KeyStore.getInstance(keyFileType);
        FileInputStream ksin = new FileInputStream(keyFileLocation);
        try {
            ks.load(ksin, keyFilePassword.trim().toCharArray());
        } finally {
            ksin.close();
        }
        return ks;
    }

    public String getHandlerName() {
        return SSLCONTEXT_HANDLER_NAME;
    }

    private KeyManagerFactory initKeyManagerFactory(String keyFileType, String keyFileLocation,
            String keyFilePasswordPath) throws SecurityException, KeyStoreException, NoSuchAlgorithmException,
            CertificateException, IOException, UnrecoverableKeyException, InvalidKeySpecException {
        KeyManagerFactory kmf = null;

        if (Strings.isNullOrEmpty(keyFileLocation)) {
            LOG.error("Key store location cannot be empty when Mutual Authentication is enabled!");
            throw new SecurityException("Key store location cannot be empty when Mutual Authentication is enabled!");
        }

        String keyFilePassword = "";
        if (!Strings.isNullOrEmpty(keyFilePasswordPath)) {
            keyFilePassword = getPasswordFromFile(keyFilePasswordPath);
        }

        // Initialize key file
        KeyStore ks = loadKeyFile(keyFileType, keyFileLocation, keyFilePassword);
        kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(ks, keyFilePassword.trim().toCharArray());

        return kmf;
    }

    private TrustManagerFactory initTrustManagerFactory(String trustFileType, String trustFileLocation,
            String trustFilePasswordPath)
            throws KeyStoreException, NoSuchAlgorithmException, CertificateException, IOException, SecurityException {
        TrustManagerFactory tmf;

        if (Strings.isNullOrEmpty(trustFileLocation)) {
            LOG.error("Trust Store location cannot be empty!");
            throw new SecurityException("Trust Store location cannot be empty!");
        }

        String trustFilePassword = "";
        if (!Strings.isNullOrEmpty(trustFilePasswordPath)) {
            trustFilePassword = getPasswordFromFile(trustFilePasswordPath);
        }

        // Initialize trust file
        KeyStore ts = loadKeyFile(trustFileType, trustFileLocation, trustFilePassword);
        tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(ts);

        return tmf;
    }

    private SslProvider getSslProvider(String sslProvider) {
        if (sslProvider.trim().equalsIgnoreCase("OpenSSL")) {
            if (OpenSsl.isAvailable()) {
                LOG.info("Security provider - OpenSSL");
                return SslProvider.OPENSSL;
            }

            Throwable causeUnavailable = OpenSsl.unavailabilityCause();
            LOG.warn("Openssl Unavailable: ", causeUnavailable);

            LOG.info("Security provider - JDK");
            return SslProvider.JDK;
        }

        LOG.info("Security provider - JDK");
        return SslProvider.JDK;
    }

    private void createClientContext(AbstractConfiguration conf)
            throws SecurityException, KeyStoreException, NoSuchAlgorithmException, CertificateException, IOException,
            UnrecoverableKeyException, InvalidKeySpecException, NoSuchProviderException {
        final SslContextBuilder sslContextBuilder;
        final ClientConfiguration clientConf;
        final SslProvider provider;
        final boolean clientAuthentication;

        // get key-file and trust-file locations and passwords
        if (!(conf instanceof ClientConfiguration)) {
            throw new SecurityException("Client configruation not provided");
        }

        clientConf = (ClientConfiguration) conf;
        provider = getSslProvider(clientConf.getSSLProvider());
        clientAuthentication = clientConf.getSSLClientAuthentication();

        switch (KeyFileType.valueOf(clientConf.getSSLTrustFileType())) {
        case PEM:
            if (Strings.isNullOrEmpty(clientConf.getSSLTrustFilePath())) {
                throw new SecurityException("CA Certificate required");
            }

            sslContextBuilder = SslContextBuilder.forClient()
                    .trustManager(new File(clientConf.getSSLTrustFilePath()))
                    .ciphers(null)
                    .sessionCacheSize(0)
                    .sessionTimeout(0)
                    .sslProvider(provider)
                    .clientAuth(ClientAuth.REQUIRE);

            break;
        case JKS:
            // falling thru, same as PKCS12
        case PKCS12:
            TrustManagerFactory tmf = initTrustManagerFactory(clientConf.getSSLTrustFileType(),
                    clientConf.getSSLTrustFilePath(), clientConf.getSSLTrustFilePasswordPath());

            sslContextBuilder = SslContextBuilder.forClient()
                    .trustManager(tmf)
                    .ciphers(null)
                    .sessionCacheSize(0)
                    .sessionTimeout(0)
                    .sslProvider(provider)
                    .clientAuth(ClientAuth.REQUIRE);

            break;
        default:
            throw new SecurityException("Invalid Trustfile type: " + clientConf.getSSLTrustFileType());
        }

        if (clientAuthentication) {
            switch (KeyFileType.valueOf(clientConf.getSSLKeyFileType())) {
            case PEM:
                final String keyPassword;

                if (Strings.isNullOrEmpty(clientConf.getSSLCertificatePath())) {
                    throw new SecurityException("Valid Certificate is missing");
                }

                if (Strings.isNullOrEmpty(clientConf.getSSLKeyFilePath())) {
                    throw new SecurityException("Valid Key is missing");
                }

                if (!Strings.isNullOrEmpty(clientConf.getSSLKeyFilePasswordPath())) {
                    keyPassword = getPasswordFromFile(clientConf.getSSLKeyFilePasswordPath());
                } else {
                    keyPassword = null;
                }

                sslContextBuilder.keyManager(new File(clientConf.getSSLCertificatePath()),
                        new File(clientConf.getSSLKeyFilePath()), keyPassword);
                break;
            case JKS:
                // falling thru, same as PKCS12
            case PKCS12:
                KeyManagerFactory kmf = initKeyManagerFactory(clientConf.getSSLKeyFileType(),
                        clientConf.getSSLKeyFilePath(), clientConf.getSSLKeyFilePasswordPath());

                sslContextBuilder.keyManager(kmf);
                break;
            default:
                throw new SecurityException("Invalid Keyfile type" + clientConf.getSSLKeyFileType());
            }
        }

        sslContext = sslContextBuilder.build();
    }

    private void createServerContext(AbstractConfiguration conf) throws SecurityException, KeyStoreException,
            NoSuchAlgorithmException, CertificateException, IOException, UnrecoverableKeyException,
            InvalidKeySpecException, NoSuchProviderException {
        final SslContextBuilder sslContextBuilder;
        final ServerConfiguration serverConf;
        final SslProvider provider;
        final boolean clientAuthentication;

        // get key-file and trust-file locations and passwords
        if (!(conf instanceof ServerConfiguration)) {
            throw new SecurityException("Server configruation not provided");
        }

        serverConf = (ServerConfiguration) conf;
        provider = getSslProvider(serverConf.getSSLProvider());
        clientAuthentication = serverConf.getSSLClientAuthentication();

        switch (KeyFileType.valueOf(serverConf.getSSLKeyFileType())) {
        case PEM:
            final String keyPassword;

            if (Strings.isNullOrEmpty(serverConf.getSSLKeyFilePath())) {
                throw new SecurityException("Key path is required");
            }

            if (Strings.isNullOrEmpty(serverConf.getSSLCertificatePath())) {
                throw new SecurityException("Certificate path is required");
            }

            if (!Strings.isNullOrEmpty(serverConf.getSSLKeyFilePasswordPath())) {
                keyPassword = getPasswordFromFile(serverConf.getSSLKeyFilePasswordPath());
            } else {
                keyPassword = null;
            }

            sslContextBuilder = SslContextBuilder
                                .forServer(new File(serverConf.getSSLCertificatePath()), 
                            new File(serverConf.getSSLKeyFilePath()), keyPassword)
                                .ciphers(null)
                                .sessionCacheSize(0)
                                .sessionTimeout(0)
                                .sslProvider(provider)
                                .startTls(true);

            break;
        case JKS:
            // falling thru, same as PKCS12
        case PKCS12:
            KeyManagerFactory kmf = initKeyManagerFactory(serverConf.getSSLKeyFileType(),
                    serverConf.getSSLKeyFilePath(),
                    serverConf.getSSLKeyFilePasswordPath());

            sslContextBuilder = SslContextBuilder.forServer(kmf)
                                .ciphers(null)
                                .sessionCacheSize(0)
                                .sessionTimeout(0)
                                .sslProvider(provider)
                                .startTls(true);

            break;
        default:
            throw new SecurityException("Invalid Keyfile type" + serverConf.getSSLKeyFileType());
        }

        if (clientAuthentication) {
            sslContextBuilder.clientAuth(ClientAuth.REQUIRE);

            switch (KeyFileType.valueOf(serverConf.getSSLTrustFileType())) {
            case PEM:
                if (Strings.isNullOrEmpty(serverConf.getSSLTrustFilePath())) {
                    throw new SecurityException("CA Certificate chain is required");
                }
                sslContextBuilder.trustManager(new File(serverConf.getSSLTrustFilePath()));
                break;
            case JKS:
                // falling thru, same as PKCS12
            case PKCS12:
                TrustManagerFactory tmf = initTrustManagerFactory(serverConf.getSSLTrustFileType(),
                        serverConf.getSSLTrustFilePath(), serverConf.getSSLTrustFilePasswordPath());
                sslContextBuilder.trustManager(tmf);
                break;
            default:
                throw new SecurityException("Invalid Trustfile type" + serverConf.getSSLTrustFileType());
            }
        }

        sslContext = sslContextBuilder.build();
    }

    @Override
    public synchronized void init(NodeType type, AbstractConfiguration conf) throws SecurityException {
        String enabledProtocols = "";
        String enabledCiphers = "";

        enabledCiphers = conf.getSslEnabledCipherSuites();
        enabledProtocols = conf.getSslEnabledProtocols();

        try {
            switch (type) {
            case Client:
                createClientContext(conf);
                break;
            case Server:
                createServerContext(conf);
                break;
            default:
                throw new SecurityException(new IllegalArgumentException("Invalid NodeType"));
            }

            if (enabledProtocols != null && !enabledProtocols.isEmpty()) {
                protocols = enabledProtocols.split(",");
            }

            if (enabledCiphers != null && !enabledCiphers.isEmpty()) {
                ciphers = enabledCiphers.split(",");
            }
        } catch (KeyStoreException e) {
            throw new RuntimeException("Standard keyfile type missing", e);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Standard algorithm missing", e);
        } catch (CertificateException e) {
            throw new SecurityException("Unable to load keyfile", e);
        } catch (IOException e) {
            throw new SecurityException("Error initializing SSLContext", e);
        } catch (UnrecoverableKeyException e) {
            throw new SecurityException("Unable to load key manager, possibly bad password", e);
        } catch (InvalidKeySpecException e) {
            throw new SecurityException("Unable to load key manager", e);
        } catch (NoSuchProviderException e) {
            throw new SecurityException("No such provider", e);
        }
    }

    @Override
    public SslHandler newSslHandler() {
        SslHandler sslHandler = sslContext.newHandler(PooledByteBufAllocator.DEFAULT);

        if (protocols != null && protocols.length != 0) {
            sslHandler.engine().setEnabledProtocols(protocols);
        }
        LOG.info("Enabled cipher protocols: {} ", Arrays.toString(sslHandler.engine().getEnabledProtocols()));

        if (ciphers != null && ciphers.length != 0) {
            sslHandler.engine().setEnabledCipherSuites(ciphers);
        }
        LOG.info("Enabled cipher suites: {} ", Arrays.toString(sslHandler.engine().getEnabledCipherSuites()));

        return sslHandler;
    }
}
