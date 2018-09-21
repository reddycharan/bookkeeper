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
package org.apache.bookkeeper.common.tls;

import com.google.common.base.Strings;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.*;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.spec.InvalidKeySpecException;
import javax.crypto.NoSuchPaddingException;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.eclipse.jetty.util.ssl.SslContextFactory;

import static org.apache.bookkeeper.common.tls.TLSUtils.getCertificates;
import static org.apache.bookkeeper.common.tls.TLSUtils.getPasswordFromFile;
import static org.apache.bookkeeper.common.tls.TLSUtils.getPrivateKey;

/**
 * Creates SslContextFactory necessary to build SSL server connector
 * for Jetty Service.
 */
@Accessors(chain = true)
@Setter
public class JettyTlsContextFactoryBuilder {
    private enum KeyStoreType {
        PEM, JKS, PKCS12
    }

    private String keyStorePath;
    private String keyStorePasswordPath;
    private String certificatePath;
    private String trustStorePath;
    private String trustStorePasswordPath;
    private Boolean clientAuthentication;
    private KeyStoreType keyStoreType;
    private KeyStoreType trustStoreType;

    public JettyTlsContextFactoryBuilder setKeyStoreType(String keyStoreType) {
        this.keyStoreType = this.keyStoreType.valueOf(keyStoreType);
        return this;
    }

    public JettyTlsContextFactoryBuilder setTrustStoreType(String trustStoreType) {
        this.trustStoreType = keyStoreType.valueOf(trustStoreType);
        return this;
    }

    public SslContextFactory build() throws KeyStoreException, IOException, InvalidKeySpecException, KeyException,
            NoSuchAlgorithmException, NoSuchPaddingException, InvalidAlgorithmParameterException, CertificateException,
            UnrecoverableKeyException {
        final SslContextFactory sslContextFactory = new SslContextFactory();
        final SSLContext sslContext = buildSslContext();

        sslContextFactory.setSslContext(sslContext);

        if (this.clientAuthentication) {
            sslContextFactory.setWantClientAuth(true);
            sslContextFactory.setValidatePeerCerts(true);
            sslContextFactory.setValidateCerts(true);
        }

        return sslContextFactory;
    }

    public SSLContext buildSslContext() throws KeyStoreException, IOException, InvalidKeySpecException, KeyException,
            NoSuchAlgorithmException, NoSuchPaddingException, InvalidAlgorithmParameterException, CertificateException,
            UnrecoverableKeyException {
        final KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        final KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
        final String keyStorePassword = getPasswordFromFile(this.keyStorePasswordPath);
        switch (this.keyStoreType) {
            case PEM:
                final String password;

                keyStore.load(null, null);

                X509Certificate certificate = (X509Certificate) CertificateFactory.getInstance("X.509")
                        .generateCertificate(new BufferedInputStream(new FileInputStream(this.certificatePath)));

                if (Strings.isNullOrEmpty(this.keyStorePasswordPath)) {
                    password = null;
                } else {
                    password = getPasswordFromFile(this.keyStorePasswordPath);
                }
                PrivateKey privateKey = getPrivateKey(this.keyStorePath, password);

                keyStore.setKeyEntry("server-key", privateKey, keyStorePassword.toCharArray(),
                        new X509Certificate[] {certificate});

                break;
            case JKS:
            case PKCS12:
                keyStore.load(new BufferedInputStream(new FileInputStream(this.keyStorePath)),
                        keyStorePassword.toCharArray());

                break;
            default:
                throw new InvalidParameterException("Unknown Key Store Type: " + this.keyStoreType);
        }
        kmf.init(keyStore, keyStorePassword.toCharArray());

        final TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        final KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
        final String trustStorePassword = getPasswordFromFile(this.trustStorePasswordPath);
        switch (this.trustStoreType) {
            case PEM:
                trustStore.load(null, null);

                X509Certificate[] trustChain = getCertificates(this.trustStorePath);
                int counter = 0;
                for (X509Certificate cert: trustChain) {
                    trustStore.setCertificateEntry(Integer.toString(counter++), cert);
                }

                break;
            case JKS:
            case PKCS12:
                keyStore.load(new BufferedInputStream(new FileInputStream(this.trustStorePath)),
                        trustStorePassword.toCharArray());

                break;
            default:
                throw new InvalidParameterException("Unknown Key Store Type: " + this.trustStoreType);
        }
        tmf.init(trustStore);

        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(kmf.getKeyManagers(), tmf.getTrustManagers(), new SecureRandom());

        return sslContext;
    }
}
