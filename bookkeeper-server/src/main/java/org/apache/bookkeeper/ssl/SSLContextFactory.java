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

import java.io.IOException;
import java.security.InvalidAlgorithmParameterException;
import java.security.KeyException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.security.spec.InvalidKeySpecException;
import java.util.Arrays;
import javax.crypto.NoSuchPaddingException;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;

import com.google.common.base.Strings;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.OpenSsl;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.SslProvider;
import org.apache.bookkeeper.conf.AbstractConfiguration;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SSLContextFactory implements SecurityHandlerFactory {
    public enum KeyFileType {
        PKCS12, JKS, PEM
    }

    private final static Logger LOG = LoggerFactory.getLogger(SSLContextFactory.class);
    private final static String SSLCONTEXT_HANDLER_NAME = "ssl";
    private String[] protocols;
    private String[] ciphers;
    private AbstractConfiguration conf;
    private NodeType nodeType;

    public String getHandlerName() {
        return SSLCONTEXT_HANDLER_NAME;
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

    private SslContext createClientContext(AbstractConfiguration conf)
            throws SecurityException, KeyStoreException, NoSuchAlgorithmException, CertificateException, IOException,
            UnrecoverableKeyException, InvalidKeySpecException, InvalidAlgorithmParameterException, KeyException,
            NoSuchPaddingException {
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

            X509Certificate[] trustChain = SSLUtils.getCertificates(clientConf.getSSLTrustFilePath());
            LOG.info("Using Trust Chain: {}", SSLUtils.prettyPrintCertChain(trustChain));

            sslContextBuilder = SslContextBuilder.forClient()
                    .trustManager(trustChain)
                    .ciphers(null)
                    .sessionCacheSize(0)
                    .sessionTimeout(0)
                    .sslProvider(provider)
                    .clientAuth(ClientAuth.REQUIRE);


            break;
        case JKS:
            // falling thru, same as PKCS12
        case PKCS12:
            TrustManagerFactory tmf = SSLUtils.initTrustManagerFactory(clientConf.getSSLTrustFileType(),
                    clientConf.getSSLTrustFilePath(), clientConf.getSSLTrustFilePasswordPath());
            LOG.info("Using Trust Chain: {}", tmf);

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
                    keyPassword = SSLUtils.getPasswordFromFile(clientConf.getSSLKeyFilePasswordPath());
                } else {
                    keyPassword = null;
                }

                X509Certificate[] certificates = SSLUtils.getCertificates(clientConf.getSSLCertificatePath());
                PrivateKey privateKey = SSLUtils.getPrivateKey(clientConf.getSSLKeyFilePath(), keyPassword);
                LOG.info("Using Credentials: {}", SSLUtils.prettyPrintCertChain(certificates));

                sslContextBuilder.keyManager(privateKey, certificates);

                break;
            case JKS:
                // falling thru, same as PKCS12
            case PKCS12:
                KeyManagerFactory kmf = SSLUtils.initKeyManagerFactory(clientConf.getSSLKeyFileType(),
                        clientConf.getSSLKeyFilePath(), clientConf.getSSLKeyFilePasswordPath());
                LOG.info("Using Credentials: {}", kmf);

                sslContextBuilder.keyManager(kmf);
                break;
            default:
                throw new SecurityException("Invalid Keyfile type: " + clientConf.getSSLKeyFileType());
            }
        }

        return sslContextBuilder.build();
    }

    private SslContext createServerContext(AbstractConfiguration conf) throws SecurityException, KeyStoreException,
            NoSuchAlgorithmException, CertificateException, IOException, UnrecoverableKeyException,
            InvalidKeySpecException, InvalidAlgorithmParameterException, KeyException, NoSuchPaddingException {
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
                keyPassword = SSLUtils.getPasswordFromFile(serverConf.getSSLKeyFilePasswordPath());
            } else {
                keyPassword = null;
            }

            X509Certificate[] certificates = SSLUtils.getCertificates(serverConf.getSSLCertificatePath());
            PrivateKey privateKey = SSLUtils.getPrivateKey(serverConf.getSSLKeyFilePath(), keyPassword);
            LOG.info("Using Credentials: {}", SSLUtils.prettyPrintCertChain(certificates));

            sslContextBuilder = SslContextBuilder.forServer(privateKey, certificates)
                    .ciphers(null)
                    .sessionCacheSize(0)
                    .sessionTimeout(0)
                    .sslProvider(provider)
                    .startTls(true);

            break;
        case JKS:
            // falling thru, same as PKCS12
        case PKCS12:
            KeyManagerFactory kmf = SSLUtils.initKeyManagerFactory(serverConf.getSSLKeyFileType(),
                    serverConf.getSSLKeyFilePath(),
                    serverConf.getSSLKeyFilePasswordPath());
            LOG.info("Using Credentials: {}", kmf);

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

                X509Certificate[] trustChain = SSLUtils.getCertificates(serverConf.getSSLTrustFilePath());
                LOG.info("Using Trust chain: {}", SSLUtils.prettyPrintCertChain(trustChain));
                sslContextBuilder.trustManager(trustChain);
                break;
            case JKS:
                // falling thru, same as PKCS12
            case PKCS12:
                TrustManagerFactory tmf = SSLUtils.initTrustManagerFactory(serverConf.getSSLTrustFileType(),
                        serverConf.getSSLTrustFilePath(), serverConf.getSSLTrustFilePasswordPath());
                LOG.info("Using Trust chain: {}", tmf);
                sslContextBuilder.trustManager(tmf);
                break;
            default:
                throw new SecurityException("Invalid Trustfile type" + serverConf.getSSLTrustFileType());
            }
        }

        return sslContextBuilder.build();
    }

    @Override
    public synchronized void init(NodeType type, AbstractConfiguration conf) {
        this.conf = conf;
        this.nodeType = type;

        String enabledProtocols;
        String enabledCiphers;

        enabledCiphers = conf.getSslEnabledCipherSuites();
        enabledProtocols = conf.getSslEnabledProtocols();

        if (enabledProtocols != null && !enabledProtocols.isEmpty()) {
            protocols = enabledProtocols.split(",");
        }

        if (enabledCiphers != null && !enabledCiphers.isEmpty()) {
            ciphers = enabledCiphers.split(",");
        }
    }

    private SslContext createSSLContext()  throws SecurityException {
        try {
            switch (nodeType) {
            case Client:
                return createClientContext(conf);
            case Server:
                return createServerContext(conf);
            default:
                throw new SecurityException(new IllegalArgumentException("Invalid NodeType"));
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
        } catch (KeyException | NoSuchPaddingException | InvalidAlgorithmParameterException e) {
            throw new SecurityException("Invalid Key file", e);
        }
    }

    @Override
    public SslHandler newSslHandler() {
        SslContext sslContext;
        try {
            sslContext = createSSLContext();
        } catch (SecurityException e) {
            LOG.error("Failed to create SSL Context: ", e);
            return null;
        }

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
