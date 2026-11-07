package io.github.dzdcache.discovery;

import java.io.ByteArrayOutputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.LoggerFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Timeout(15)
class DnsPeerDirectoryTest {
    private static final String NAME = "dzdcache-peers.testing.svc.cluster.local";

    @Test void resolvesEveryAddressOfTheServiceNameExceptItsOwn() throws Exception {
        try (FakeDns dns = new FakeDns()) {
            dns.records.put(NAME, List.of("10.0.0.1", "10.0.0.2", "2001:db8::3"));
            DiscoveryProperties properties = properties();
            properties.setPodIp("10.0.0.1");
            properties.setPeerPort(9090);
            try (var directory = new DnsPeerDirectory(properties, dns.address())) {
                assertThat(directory.snapshot().available()).isFalse();
                directory.start();
                await(() -> directory.snapshot().available());
                assertThat(directory.snapshot().peers().values()).containsExactlyInAnyOrder(
                    URI.create("http://10.0.0.2:9090"), URI.create("http://[2001:db8::3]:9090"));
            }
        }
    }

    @Test void aNameWithoutAddressesYetMeansNoPeers() throws Exception {
        try (FakeDns dns = new FakeDns(); var directory = new DnsPeerDirectory(properties(), dns.address())) {
            directory.start();
            await(() -> directory.snapshot().available());
            assertThat(directory.snapshot().peers()).isEmpty();
        }
    }

    @Test void everyRefreshAsksTheServerAgainSoNewPodsAppearAtOnce() throws Exception {
        try (FakeDns dns = new FakeDns()) {
            dns.records.put(NAME, List.of("10.0.0.1"));
            DiscoveryProperties properties = properties();
            properties.setPodIp("10.0.0.1");
            try (var directory = new DnsPeerDirectory(properties, dns.address())) {
                directory.start();
                await(() -> directory.snapshot().available());
                long generation = directory.snapshot().generation();
                dns.records.put(NAME, List.of("10.0.0.1", "10.0.0.4"));
                directory.refresh();
                await(() -> directory.snapshot().generation() > generation);
                assertThat(directory.snapshot().peers().values()).containsExactly(URI.create("http://10.0.0.4:7070"));
            }
        }
    }

    @Test void asksForAAndAaaaSeparatelyBecauseClusterDnsAnswersAnAnyQueryWithoutAddresses() throws Exception {
        try (FakeDns dns = new FakeDns()) {
            dns.records.put(NAME, List.of("10.0.0.2", "2001:db8::3"));
            try (var directory = new DnsPeerDirectory(properties(), dns.address())) {
                directory.start();
                await(() -> directory.snapshot().available());
                assertThat(directory.snapshot().peers().values()).containsExactlyInAnyOrder(
                    URI.create("http://10.0.0.2:7070"), URI.create("http://[2001:db8::3]:7070"));
            }
            assertThat(dns.queried).containsExactly(FakeDns.A, FakeDns.AAAA);
        }
    }

    @Test void anEmptyAnswerIsLoggedOncePerMinuteAndAgainAsSoonAsItReturnsAfterAddresses() throws Exception {
        Logger logger = (Logger) LoggerFactory.getLogger(DnsPeerDirectory.class);
        ListAppender<ILoggingEvent> warnings = new ListAppender<>();
        warnings.start();
        logger.addAppender(warnings);
        MovableClock clock = new MovableClock();
        try (FakeDns dns = new FakeDns(); var directory = new DnsPeerDirectory(properties(), dns.address(), clock)) {
            directory.start();
            await(() -> clock.now.equals(directory.snapshot().discoveredAt()));
            assertThat(warnings.list).extracting(ILoggingEvent::getFormattedMessage)
                .containsExactly("DNS returned no A or AAAA records for " + NAME + ", so this node finds no peers");
            discoverAt(directory, clock, clock.now.plusSeconds(59));
            assertThat(warnings.list).hasSize(1);
            discoverAt(directory, clock, clock.now.plusSeconds(1));
            assertThat(warnings.list).hasSize(2);
            dns.records.put(NAME, List.of("10.0.0.2"));
            discoverAt(directory, clock, clock.now.plusSeconds(1));
            assertThat(directory.snapshot().peers()).hasSize(1);
            dns.records.remove(NAME);
            discoverAt(directory, clock, clock.now.plusSeconds(1));
            assertThat(warnings.list).hasSize(3);
        } finally {
            logger.detachAppender(warnings);
        }
    }

    @Test void theNameDefaultsToThePeerServiceInItsNamespace() {
        DiscoveryProperties properties = new DiscoveryProperties();
        properties.setServiceName("cache-peers");
        properties.setNamespace("pricing");
        assertThat(properties.getDnsName()).isEqualTo("cache-peers.pricing.svc.cluster.local");
        properties.setDnsName("cache-peers.pricing.svc.example.internal");
        assertThat(properties.getDnsName()).isEqualTo("cache-peers.pricing.svc.example.internal");
    }

    @Test void anInvalidNameIsRefused() {
        DiscoveryProperties properties = properties();
        properties.setDnsName("not a name");
        assertThatThrownBy(() -> new DnsPeerDirectory(properties)).isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("dns-name");
    }

    private static DiscoveryProperties properties() {
        DiscoveryProperties properties = new DiscoveryProperties();
        properties.setMode("dns");
        properties.setNamespace("testing");
        properties.setPodIp("10.0.0.99");
        properties.setRetryDelay(java.time.Duration.ofMillis(10));
        return properties;
    }

    private static void discoverAt(DnsPeerDirectory directory, MovableClock clock, Instant at) throws InterruptedException {
        clock.now = at;
        directory.refresh();
        await(() -> at.equals(directory.snapshot().discoveredAt()));
    }

    private static final class MovableClock extends Clock {
        volatile Instant now = Instant.parse("2026-11-07T10:00:00Z");
        @Override public java.time.ZoneId getZone() { return ZoneOffset.UTC; }
        @Override public Clock withZone(java.time.ZoneId zone) { return this; }
        @Override public Instant instant() { return now; }
    }

    private static void await(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + 10_000_000_000L;
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) throw new AssertionError("condition not reached");
            Thread.sleep(10);
        }
    }

    private static final class FakeDns implements AutoCloseable {
        static final int A = 1, AAAA = 28, ANY = 255, HINFO = 13;
        final Map<String, List<String>> records = new ConcurrentHashMap<>();
        final List<Integer> queried = new CopyOnWriteArrayList<>();
        private final DatagramSocket socket;
        private final Thread worker;

        FakeDns() throws Exception {
            socket = new DatagramSocket(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
            worker = Thread.ofVirtual().start(this::serve);
        }

        String address() { return "127.0.0.1:" + socket.getLocalPort(); }

        private void serve() {
            byte[] buffer = new byte[512];
            while (!socket.isClosed()) {
                try {
                    DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                    socket.receive(packet);
                    byte[] answer = answer(ByteBuffer.wrap(packet.getData(), 0, packet.getLength()));
                    socket.send(new DatagramPacket(answer, answer.length, packet.getSocketAddress()));
                } catch (Exception e) {
                    if (!socket.isClosed()) throw new IllegalStateException(e);
                }
            }
        }

        private byte[] answer(ByteBuffer query) throws Exception {
            short id = query.getShort();
            query.position(12);
            int questionStart = query.position();
            StringBuilder name = new StringBuilder();
            for (int length = query.get() & 0xff; length > 0; length = query.get() & 0xff) {
                byte[] label = new byte[length];
                query.get(label);
                if (!name.isEmpty()) name.append('.');
                name.append(new String(label, StandardCharsets.US_ASCII));
            }
            int type = query.getShort() & 0xffff;
            queried.add(type);
            query.getShort();
            byte[] question = new byte[query.position() - questionStart];
            query.position(questionStart);
            query.get(question);
            List<String> found = records.get(name.toString().toLowerCase());
            List<InetAddress> addresses = found == null || type == ANY ? List.of() : found.stream().map(FakeDns::parse)
                .filter(address -> (type == A) == (address.getAddress().length == 4)).toList();
            boolean rfc8482 = found != null && type == ANY;
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            ByteBuffer header = ByteBuffer.allocate(12);
            header.putShort(id).putShort((short) (found == null ? 0x8183 : 0x8180)).putShort((short) 1)
                .putShort((short) (rfc8482 ? 1 : addresses.size())).putShort((short) 0).putShort((short) 0);
            out.write(header.array());
            out.write(question);
            if (rfc8482) {
                byte[] cpu = "RFC8482".getBytes(StandardCharsets.US_ASCII);
                ByteBuffer record = ByteBuffer.allocate(12 + 2 + cpu.length);
                record.putShort((short) 0xc00c).putShort((short) HINFO).putShort((short) 1)
                    .putInt(3600).putShort((short) (2 + cpu.length)).put((byte) cpu.length).put(cpu).put((byte) 0);
                out.write(record.array());
            }
            for (InetAddress address : addresses) {
                byte[] raw = address.getAddress();
                ByteBuffer record = ByteBuffer.allocate(12 + raw.length);
                record.putShort((short) 0xc00c).putShort((short) (raw.length == 4 ? A : AAAA)).putShort((short) 1)
                    .putInt(30).putShort((short) raw.length).put(raw);
                out.write(record.array());
            }
            return out.toByteArray();
        }

        private static InetAddress parse(String ip) {
            try { return InetAddress.getByName(ip); } catch (Exception e) { throw new IllegalArgumentException(e); }
        }

        @Override public void close() throws Exception {
            socket.close();
            worker.join();
        }
    }
}
