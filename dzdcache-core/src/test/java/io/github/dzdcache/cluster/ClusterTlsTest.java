package io.github.dzdcache.cluster;

import io.github.dzdcache.cache.CacheEngine;
import io.github.dzdcache.cache.EngineHolder;
import io.github.dzdcache.config.ServiceProperties;
import io.github.dzdcache.discovery.Membership;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.KeyFactory;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ClusterTlsTest {
    private final JsonMapper mapper = JsonMapper.builder().build();
    private final ServiceProperties properties = new ServiceProperties();
    private final Member self = new Member("a", URI.create("https://127.0.0.1:1"), 1);
    private final List<AutoCloseable> resources = new ArrayList<>();

    @AfterEach void close() throws Exception { for (AutoCloseable resource : resources.reversed()) resource.close(); }

    private int server(String identity, boolean needClientAuth) throws Exception {
        properties.setClusterToken("secret");
        var timers = keep(new SerialTimers());
        var coordinator = keep(new ClusterCoordinator(new EngineHolder(() -> new CacheEngine(null, mapper, 10, 100)),
            () -> new Membership(Map.of(), 1, true), (peer, message) -> null, self, properties, new SimpleMeterRegistry(),
            timers, new java.util.SplittableRandom(1), mapper));
        var server = keep(new ClusterServer(new InetSocketAddress("127.0.0.1", 0), coordinator, mapper, properties,
            PeerTrust.context("TLS", keys(identity), trust(), true), needClientAuth));
        server.start();
        return server.port();
    }

    private TcpRaftTransport client(String identity, boolean verifyHostname) throws Exception {
        return keep(new TcpRaftTransport(mapper, properties, PeerTrust.context("TLS", identity == null ? null : keys(identity), trust(), verifyHostname)));
    }

    private <T extends AutoCloseable> T keep(T resource) { resources.add(resource); return resource; }

    private Message whoLeads(RaftTransport transport, String uri) throws Exception {
        return transport.send(URI.create(uri), new Message.WhoLeads()).get(5, TimeUnit.SECONDS);
    }

    @Test void transportAndServerTalkOverTls() throws Exception {
        int port = server("node", false);
        var transport = client(null, true);
        assertThat(whoLeads(transport, "https://127.0.0.1:" + port)).isInstanceOf(Message.Leader.class);
        assertThat(whoLeads(transport, "https://localhost:" + port)).isInstanceOf(Message.Leader.class);
    }

    @Test void aPlainHttpClientCannotTalkToATlsServerAndATlsClientRefusesHttpPeers() throws Exception {
        int port = server("node", false);
        var plain = keep(new TcpRaftTransport(mapper, properties));
        assertThatThrownBy(() -> whoLeads(plain, "http://127.0.0.1:" + port)).isInstanceOf(ExecutionException.class);
        assertThatThrownBy(() -> whoLeads(client(null, true), "http://127.0.0.1:" + port))
            .isInstanceOf(ExecutionException.class).hasMessageContaining("must be addressed with https://");
    }

    @Test void mutualTlsRequiresATrustedClientCertificate() throws Exception {
        int port = server("node", true);
        String uri = "https://127.0.0.1:" + port;
        assertThatThrownBy(() -> whoLeads(client(null, true), uri)).isInstanceOf(ExecutionException.class);
        assertThatThrownBy(() -> whoLeads(client("untrusted", true), uri)).isInstanceOf(ExecutionException.class);
        assertThat(whoLeads(client("node", true), uri)).isInstanceOf(Message.Leader.class);
    }

    @Test void theClientRejectsAServerCertificateFromAnUntrustedIssuerEvenWithoutHostnameVerification() throws Exception {
        int port = server("untrusted", false);
        assertThatThrownBy(() -> whoLeads(client(null, true), "https://127.0.0.1:" + port))
            .isInstanceOf(ExecutionException.class).hasStackTraceContaining("PKIX");
        assertThatThrownBy(() -> whoLeads(client(null, false), "https://127.0.0.1:" + port))
            .isInstanceOf(ExecutionException.class).hasStackTraceContaining("PKIX");
    }

    @Test void hostnameVerificationCanBeSwitchedOffForPeersAddressedByIp() throws Exception {
        int port = server("other-host", false);
        assertThatThrownBy(() -> whoLeads(client(null, true), "https://127.0.0.1:" + port))
            .isInstanceOf(ExecutionException.class).hasStackTraceContaining("No subject alternative names matching IP address 127.0.0.1");
        assertThat(whoLeads(client(null, false), "https://127.0.0.1:" + port)).isInstanceOf(Message.Leader.class);
    }

    private static KeyManager[] keys(String name) throws Exception {
        byte[] key = Base64.getMimeDecoder().decode(pem(name + "-key.pem").replaceAll("-----[A-Z ]+-----", ""));
        KeyStore store = KeyStore.getInstance("PKCS12"); store.load(null);
        store.setKeyEntry(name, KeyFactory.getInstance("EC").generatePrivate(new PKCS8EncodedKeySpec(key)), new char[0],
            certificates(name + ".pem").toArray(Certificate[]::new));
        var factory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm()); factory.init(store, new char[0]);
        return factory.getKeyManagers();
    }

    private static TrustManager[] trust() throws Exception {
        KeyStore store = KeyStore.getInstance("PKCS12"); store.load(null);
        store.setCertificateEntry("ca", certificates("ca.pem").getFirst());
        var factory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm()); factory.init(store);
        return factory.getTrustManagers();
    }

    private static List<Certificate> certificates(String file) throws Exception {
        return List.copyOf(CertificateFactory.getInstance("X.509")
            .generateCertificates(new ByteArrayInputStream(pem(file).getBytes(StandardCharsets.US_ASCII))));
    }

    private static String pem(String file) throws IOException {
        try (InputStream in = ClusterTlsTest.class.getResourceAsStream("/tls/" + file)) {
            return new String(in.readAllBytes(), StandardCharsets.US_ASCII);
        }
    }
}
