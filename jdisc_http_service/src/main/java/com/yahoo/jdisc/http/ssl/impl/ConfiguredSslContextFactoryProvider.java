// Copyright 2019 Oath Inc. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
package com.yahoo.jdisc.http.ssl.impl;

import com.yahoo.jdisc.http.ConnectorConfig;
import com.yahoo.jdisc.http.ssl.SslContextFactoryProvider;
import com.yahoo.security.KeyStoreBuilder;
import com.yahoo.security.KeyStoreType;
import com.yahoo.security.KeyUtils;
import com.yahoo.security.X509CertificateUtils;
import org.eclipse.jetty.util.ssl.SslContextFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.List;

/**
 * An implementation of {@link SslContextFactoryProvider} that uses the {@link ConnectorConfig} to construct a {@link SslContextFactory}.
 *
 * @author bjorncs
 */
public class ConfiguredSslContextFactoryProvider implements SslContextFactoryProvider {

    private final ConnectorConfig connectorConfig;

    public ConfiguredSslContextFactoryProvider(ConnectorConfig connectorConfig) {
        validateConfig(connectorConfig.ssl());
        this.connectorConfig = connectorConfig;
    }

    @Override
    public SslContextFactory getInstance(String containerId, int port) {
        ConnectorConfig.Ssl sslConfig = connectorConfig.ssl();
        if (!sslConfig.enabled()) throw new IllegalStateException();
        SslContextFactory.Server factory = new JDiscSslContextFactory();

        switch (sslConfig.clientAuth()) {
            case NEED_AUTH:
                factory.setNeedClientAuth(true);
                break;
            case WANT_AUTH:
                factory.setWantClientAuth(true);
                break;
        }

        // Check if using new ssl syntax from services.xml
        factory.setKeyStore(createKeystore(sslConfig));
        factory.setKeyStorePassword("");
        if (!sslConfig.caCertificateFile().isEmpty()) {
            factory.setTrustStore(createTruststore(sslConfig));
        }
        factory.setProtocol("TLS");
        return factory;
    }

    private static void validateConfig(ConnectorConfig.Ssl config) {
        if (!config.enabled()) return;

        if(hasBoth(config.certificate(), config.certificateFile()))
            throw new IllegalArgumentException("Specified both certificate and certificate file.");

        if(hasBoth(config.privateKey(), config.privateKeyFile()))
            throw new IllegalArgumentException("Specified both private key and private key file.");

        if(hasNeither(config.certificate(), config.certificateFile()))
            throw new IllegalArgumentException("Specified neither certificate or certificate file.");

        if(hasNeither(config.privateKey(), config.privateKeyFile()))
            throw new IllegalArgumentException("Specified neither private key or private key file.");
    }

    private static boolean hasBoth(String a, String b) { return !a.isBlank() && !b.isBlank(); }
    private static boolean hasNeither(String a, String b) { return a.isBlank() && b.isBlank(); }

    private static KeyStore createTruststore(ConnectorConfig.Ssl sslConfig) {
        List<X509Certificate> caCertificates = X509CertificateUtils.certificateListFromPem(readToString(sslConfig.caCertificateFile()));
        return KeyStoreBuilder.withType(KeyStoreType.JKS)
                .withCertificateEntries("entry", caCertificates)
                .build();
    }

    private static KeyStore createKeystore(ConnectorConfig.Ssl sslConfig) {
        PrivateKey privateKey = KeyUtils.fromPemEncodedPrivateKey(getPrivateKey(sslConfig));
        List<X509Certificate> certificates = X509CertificateUtils.certificateListFromPem(getCertificate(sslConfig));
        return KeyStoreBuilder.withType(KeyStoreType.JKS).withKeyEntry("default", privateKey, certificates).build();
    }

    private static String getPrivateKey(ConnectorConfig.Ssl config) {
        if(!config.privateKey().isBlank()) return config.privateKey();
        return readToString(config.privateKeyFile());
    }

    private static String getCertificate(ConnectorConfig.Ssl config) {
        if(!config.certificate().isBlank()) return config.certificate();
        return readToString(config.certificateFile());
    }

    private static String readToString(String filename) {
        try {
            return Files.readString(Paths.get(filename), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

}
