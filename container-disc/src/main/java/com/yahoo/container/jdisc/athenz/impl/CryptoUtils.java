// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
package com.yahoo.container.jdisc.athenz.impl;

import org.bouncycastle.asn1.pkcs.PKCSObjectIdentifiers;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.ExtensionsGenerator;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.openssl.jcajce.JcaPEMWriter;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.bouncycastle.pkcs.PKCS10CertificationRequest;
import org.bouncycastle.pkcs.PKCS10CertificationRequestBuilder;
import org.bouncycastle.pkcs.jcajce.JcaPKCS10CertificationRequestBuilder;
import org.bouncycastle.util.io.pem.PemObject;

import javax.security.auth.x500.X500Principal;
import java.io.IOException;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;

/**
 * @author bjorncs
 */
class CryptoUtils {

    private CryptoUtils() {}

    static KeyPair createKeyPair() {
        try {
            KeyPairGenerator kpg = KeyPairGenerator.getInstance("RSA");
            return kpg.generateKeyPair();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    static PKCS10CertificationRequest createCSR(String identityDomain,
                                                String identityService,
                                                String dnsSuffix,
                                                String providerUniqueId,
                                                KeyPair keyPair) throws IOException {
        try {
            // Add SAN dnsname <service>.<domain-with-dashes>.<provider-dnsname-suffix>
            // and SAN dnsname <provider-unique-instance-id>.instanceid.athenz.<provider-dnsname-suffix>
            GeneralNames subjectAltNames = new GeneralNames(new GeneralName[]{
                    new GeneralName(GeneralName.dNSName, String.format("%s.%s.%s",
                                                                       identityService,
                                                                       identityDomain.replace(".", "-"),
                                                                       dnsSuffix)),
                    new GeneralName(GeneralName.dNSName, String.format("%s.instanceid.athenz.%s",
                                                                       providerUniqueId,
                                                                       dnsSuffix))
            });

            ExtensionsGenerator extGen = new ExtensionsGenerator();
            extGen.addExtension(Extension.subjectAlternativeName, false, subjectAltNames);

            X500Principal subject = new X500Principal(
                    String.format("CN=%s.%s", identityDomain, identityService));

            PKCS10CertificationRequestBuilder requestBuilder =
                    new JcaPKCS10CertificationRequestBuilder(subject, keyPair.getPublic());
            requestBuilder.addAttribute(PKCSObjectIdentifiers.pkcs_9_at_extensionRequest, extGen.generate());
            return requestBuilder.build(new JcaContentSignerBuilder("SHA256withRSA").build(keyPair.getPrivate()));
        } catch (OperatorCreationException e) {
            throw new RuntimeException(e);
        }
    }

    static String toPem(PKCS10CertificationRequest csr) {
        try (StringWriter stringWriter = new StringWriter(); JcaPEMWriter pemWriter = new JcaPEMWriter(stringWriter)) {
            pemWriter.writeObject(new PemObject("CERTIFICATE REQUEST", csr.getEncoded()));
            return stringWriter.toString();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
