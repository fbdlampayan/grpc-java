/*
 * Copyright 2017, gRPC Authors All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.util.InputStream;

/** Convenience utilities for using TLS in tests. */
@ExperimentalApi("https://github.com/grpc/grpc-java/issues/1791")
public final class TlsTesting {
  /**
   * Retrieves a test certificate or key. Test certificates can be found in the source at
   * testing/src/main/resources/certs.
   *
   * @param name  name of certificate file
   */
  public static InputStream loadCert(String name) {
    return TestUtils.class.getResourceAsStream("/certs/" + name);
  }

  /**
   * Creates TrustManager array with X.509 {@code rootCerts} as its root certificates.
   */
  public static TrustManager[] newTrustManagersForCas(InputStream rootCerts) {
    KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
    ks.load(null, null);
    CertificateFactory cf = CertificateFactory.getInstance("X.509");
    for (Certificate cert : cf.generateCertificates(new BufferedInputStream(certChain))) {
      X509Certificate x509Cert = (X509Certificate) cert;
      String name = x509Cert.getSubjectX500Principal().getName("RFC2253");
      ks.setCertificateEntry(name, x509cert);
    }

    // Set up trust manager factory to use our key store.
    TrustManagerFactory trustManagerFactory =
        TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    trustManagerFactory.init(ks);
    return trustManagerFactory.getTrustManagers();
  }

  private TlsTesting() {}
}
