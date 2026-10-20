package io.github.dzdcache.cluster;

import java.net.Socket;
import java.security.GeneralSecurityException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509ExtendedTrustManager;
import javax.net.ssl.X509TrustManager;

public final class PeerTrust {
    private PeerTrust() { }

    public static SSLContext context(String protocol, KeyManager[] keys, TrustManager[] trust, boolean verifyHostname)
            throws GeneralSecurityException {
        SSLContext context = SSLContext.getInstance(protocol);
        context.init(keys, verifyHostname || trust == null ? trust : Arrays.stream(trust).map(PeerTrust::chainOnly).toArray(TrustManager[]::new), null);
        return context;
    }

    private static TrustManager chainOnly(TrustManager manager) {
        return manager instanceof X509TrustManager x509 ? new ChainOnly(x509) : manager;
    }

    private static final class ChainOnly extends X509ExtendedTrustManager {
        private final X509TrustManager delegate;
        ChainOnly(X509TrustManager delegate) { this.delegate = delegate; }
        @Override public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
            delegate.checkClientTrusted(chain, authType);
        }
        @Override public void checkClientTrusted(X509Certificate[] chain, String authType, Socket socket) throws CertificateException {
            if (delegate instanceof X509ExtendedTrustManager extended) extended.checkClientTrusted(chain, authType, socket);
            else delegate.checkClientTrusted(chain, authType);
        }
        @Override public void checkClientTrusted(X509Certificate[] chain, String authType, SSLEngine engine) throws CertificateException {
            if (delegate instanceof X509ExtendedTrustManager extended) extended.checkClientTrusted(chain, authType, engine);
            else delegate.checkClientTrusted(chain, authType);
        }
        @Override public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
            delegate.checkServerTrusted(chain, authType);
        }
        @Override public void checkServerTrusted(X509Certificate[] chain, String authType, Socket socket) throws CertificateException {
            delegate.checkServerTrusted(chain, authType);
        }
        @Override public void checkServerTrusted(X509Certificate[] chain, String authType, SSLEngine engine) throws CertificateException {
            delegate.checkServerTrusted(chain, authType);
        }
        @Override public X509Certificate[] getAcceptedIssuers() { return delegate.getAcceptedIssuers(); }
    }
}
