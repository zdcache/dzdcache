package io.github.dzdcache.spring;

import io.github.dzdcache.cache.CacheException;
import io.github.dzdcache.cache.EngineHolder;
import io.github.dzdcache.cluster.TcpRaftTransport;
import io.github.dzdcache.cluster.Member;
import io.github.dzdcache.cluster.Message;
import io.github.dzdcache.cluster.RaftTransport;
import io.github.dzdcache.config.ServiceProperties;
import io.github.dzdcache.cluster.ClusterCoordinator;
import io.github.zdcache.IdentityDimension;
import io.github.zdcache.SearchSpecification;
import io.github.zdcache.ValueRange;
import java.net.ConnectException;
import java.net.ServerSocket;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DzdCacheStarterTest {
    public record Student(long id, String name, String city, int age) { }
    public record Course(String code, String title) { }

    @SpringBootConfiguration @EnableAutoConfiguration
    static class Students {
        @Bean CacheType<Student, Long> students() {
            return CacheType.of("students", 1, Long.class, SearchSpecification.builder(IdentityDimension.of(Student.class, "id", Student::id))
                .exact(Student::city).prefix(Student::name).range(Student::age).build()).capacity(2);
        }
    }

    @SpringBootConfiguration @Import(Students.class)
    static class StudentsAndCourses {
        @Bean CacheType<Course, String> courses() {
            return CacheType.of("courses", 1, String.class,
                SearchSpecification.builder(IdentityDimension.of(Course.class, "code", Course::code)).prefix(Course::title).build());
        }
    }

    static final Set<Integer> cutOff = ConcurrentHashMap.newKeySet();
    static final Set<String> cutLinks = ConcurrentHashMap.newKeySet();
    private static String link(int a, int b) { return Math.min(a, b) + "-" + Math.max(a, b); }

    @SpringBootConfiguration @Import(Students.class)
    static class PartitionableStudents {
        @Bean io.micrometer.core.instrument.MeterRegistry meterRegistry() { return new io.micrometer.core.instrument.simple.SimpleMeterRegistry(); }
        @Bean RaftTransport dzdcacheTransport(DzdCacheMapper mapper, ServiceProperties properties) {
            RaftTransport tcp = new TcpRaftTransport(mapper.mapper(), properties);
            int own = properties.getClusterPort();
            return (URI peer, Message message) -> cutOff.contains(own) != cutOff.contains(peer.getPort()) || cutLinks.contains(link(own, peer.getPort()))
                ? CompletableFuture.failedFuture(new ConnectException("partitioned")) : tcp.send(peer, message);
        }
    }

    private final List<ConfigurableApplicationContext> nodes = new ArrayList<>();

    @AfterEach void stop() { cutOff.clear(); cutLinks.clear(); nodes.forEach(ConfigurableApplicationContext::close); }

    private List<ConfigurableApplicationContext> start(Class<?>... configurations) throws Exception {
        return start("http", List.of(), configurations);
    }

    private List<ConfigurableApplicationContext> start(String scheme, List<String> extra, Class<?>... configurations) throws Exception {
        int[] ports = new int[configurations.length];
        for (int i = 0; i < ports.length; i++) try (var socket = new ServerSocket(0)) { ports[i] = socket.getLocalPort(); }
        List<ConfigurableApplicationContext> started = new ArrayList<>();
        for (int i = 0; i < ports.length; i++) {
            List<String> args = new ArrayList<>(List.of("--dzdcache.discovery.mode=static", "--dzdcache.discovery.node-id=node-" + i,
                "--dzdcache.cluster-port=" + ports[i], "--dzdcache.discovery.self-url=" + scheme + "://localhost:" + ports[i],
                "--dzdcache.cluster-token=secret", "--logging.level.root=WARN", "--spring.main.banner-mode=off"));
            args.addAll(extra);
            int peer = 0;
            for (int j = 0; j < ports.length; j++)
                if (j != i) args.add("--dzdcache.discovery.static-peers[" + peer++ + "]=" + scheme + "://localhost:" + ports[j]);
            ConfigurableApplicationContext node = new SpringApplicationBuilder(configurations[i]).web(WebApplicationType.NONE)
                .run(args.toArray(String[]::new));
            nodes.add(node);
            started.add(node);
            Thread.sleep(20);
        }
        await(() -> started.stream().allMatch(node -> node.getBean(DistributedCache.class).current()));
        return started;
    }

    private static void await(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) Thread.sleep(20);
        assertThat(condition.getAsBoolean()).as("condition reached in time").isTrue();
    }

    private static ClusterCache<Student, Long> students(ConfigurableApplicationContext node) {
        return node.getBean(DistributedCache.class).cache(Student.class, Long.class);
    }
    private static ClusterCoordinator cluster(ConfigurableApplicationContext node) { return node.getBean(ClusterCoordinator.class); }
    private static String fingerprint(ConfigurableApplicationContext node) {
        return node.getBean(EngineHolder.class).engine().fingerprint("students", 1);
    }

    @Test void instancesOfAServiceShareItsObjectsSearchesAndEvictions() throws Exception {
        var started = start(Students.class, Students.class, Students.class);
        await(() -> started.stream().allMatch(node -> node.getBean(EngineHolder.class).engine().contains("students", 1)));
        var a = students(started.get(0));
        var c = students(started.get(2));
        assertThat(a.put(new Student(1, "Anna", "Oslo", 22))).isEqualTo(1);
        assertThat(c.put(new Student(2, "Anders", "Bergen", 31))).isEqualTo(1);
        assertThat(c.put(new Student(2, "Anders", "Oslo", 31))).isEqualTo(2);
        await(() -> started.stream().allMatch(node -> students(node).get(2L) != null && students(node).get(2L).city().equals("Oslo")));
        assertThat(students(started.get(1)).search(Student::city, "Oslo")).extracting(Student::id).containsExactlyInAnyOrder(1L, 2L);
        assertThat(students(started.get(1)).search("age", ValueRange.between(20, 25))).containsExactly(new Student(1, "Anna", "Oslo", 22));

        a.put(new Student(3, "Berit", "Oslo", 40));
        await(() -> started.stream().map(DzdCacheStarterTest::fingerprint).distinct().count() == 1);
        assertThat(started).allSatisfy(node -> assertThat(node.getBean(EngineHolder.class).engine().entryCount("students", 1)).isEqualTo(2));
        assertThat(a.invalidate(3L)).isEqualTo(2);
        await(() -> started.stream().allMatch(node -> students(node).get(3L) == null));
    }

    private static final List<String> TLS = List.of("--dzdcache.cluster-tls.enabled=true", "--dzdcache.cluster-tls.bundle=cluster",
        "--dzdcache.cluster-tls.client-auth=need",
        "--spring.ssl.bundle.pem.cluster.keystore.certificate=classpath:tls/node.pem",
        "--spring.ssl.bundle.pem.cluster.keystore.private-key=classpath:tls/node-key.pem",
        "--spring.ssl.bundle.pem.cluster.truststore.certificate=classpath:tls/ca.pem");

    @Test void instancesFormAClusterOverMutualTlsFromAnSslBundle() throws Exception {
        var started = start("https", TLS, Students.class, Students.class, Students.class);
        assertThat(started).allSatisfy(node -> assertThat(cluster(node).self().url().getScheme()).isEqualTo("https"));
        await(() -> started.stream().allMatch(node -> node.getBean(EngineHolder.class).engine().contains("students", 1)));
        assertThat(students(started.get(0)).put(new Student(1, "Anna", "Oslo", 22))).isEqualTo(1);
        await(() -> {
            try { return new Student(1, "Anna", "Oslo", 22).equals(students(started.get(2)).get(1L)); }
            catch (RuntimeException e) { return false; }
        });
    }

    @Test void clusterTlsRefusesPlainHttpPeerAddresses() {
        List<String> args = new ArrayList<>(List.of("--dzdcache.discovery.mode=static", "--dzdcache.cluster-token=secret",
            "--dzdcache.discovery.self-url=http://localhost:7070", "--logging.level.root=OFF", "--spring.main.banner-mode=off"));
        args.addAll(TLS);
        assertThatThrownBy(() -> new SpringApplicationBuilder(Students.class).web(WebApplicationType.NONE).run(args.toArray(String[]::new)))
            .hasStackTraceContaining("dzdcache.discovery.self-url http://localhost:7070 must use https:// because dzdcache.cluster-tls.enabled is true");
    }

    @Test void typedSearchesNarrowByAPredicateAndCountInProcess() {
        try (ConfigurableApplicationContext node = new SpringApplicationBuilder(Students.class).web(WebApplicationType.NONE)
                .run("--logging.level.root=OFF", "--spring.main.banner-mode=off")) {
            await(() -> node.getBean(DistributedCache.class).current());
            ClusterCache<Student, Long> students = students(node);
            students.put(new Student(1, "Anna", "Oslo", 31));
            students.put(new Student(2, "Bo", "Oslo", 19));
            assertThat(students.search("city", student -> student.age() > 20, "Oslo")).extracting(Student::id).containsExactly(1L);
            assertThat(students.count("city", "Oslo")).isEqualTo(2);
            assertThat(students.count("city", student -> student.name().startsWith("B"), "Oslo")).isEqualTo(1);
            assertThat(students.countBy(Student::age, "city", "Oslo")).containsEntry(31, 1L).containsEntry(19, 1L);
            assertThat(students.scan(student -> student.age() < 20)).extracting(Student::id).containsExactly(2L);
        } catch (InterruptedException e) { throw new AssertionError(e); }
    }

    @Test void typedListenersReceiveEveryChangeInOrder() throws Exception {
        try (ConfigurableApplicationContext node = new SpringApplicationBuilder(Students.class).web(WebApplicationType.NONE)
                .run("--logging.level.root=OFF", "--spring.main.banner-mode=off")) {
            await(() -> node.getBean(DistributedCache.class).current());
            ClusterCache<Student, Long> students = students(node);
            List<String> seen = new java.util.concurrent.CopyOnWriteArrayList<>();
            try (var ignored = students.onChange(change -> seen.add(change.kind() + " " + change.identity()
                    + (change.value() == null ? "" : " " + change.value().name())))) {
                students.put(new Student(1, "Anna", "Oslo", 31));
                students.put(new Student(2, "Bo", "Oslo", 19));
                students.put(new Student(3, "Cy", "Oslo", 40));
                students.invalidate(2L);
                await(() -> seen.size() >= 5);
            }
            assertThat(seen).containsExactly("PUT 1 Anna", "PUT 2 Bo", "PUT 3 Cy", "EVICTED_LRU 1", "INVALIDATED 2");
        }
    }

    @Test void http2BetweenNodesNeedsAServletWebServer() {
        assertThatThrownBy(() -> new SpringApplicationBuilder(Students.class).web(WebApplicationType.NONE).run("--dzdcache.discovery.mode=static",
                "--dzdcache.cluster-token=secret", "--dzdcache.cluster-transport=http2", "--dzdcache.discovery.self-url=http://localhost:7070",
                "--logging.level.root=OFF", "--spring.main.banner-mode=off"))
            .hasStackTraceContaining("dzdcache.cluster-transport is http2, but this application runs no servlet web server");
    }

    @Test void webSocketsBetweenNodesNeedAServletWebServer() {
        assertThatThrownBy(() -> new SpringApplicationBuilder(Students.class).web(WebApplicationType.NONE).run("--dzdcache.discovery.mode=static",
                "--dzdcache.cluster-token=secret", "--dzdcache.cluster-transport=websocket", "--dzdcache.discovery.self-url=http://localhost:7070",
                "--logging.level.root=OFF", "--spring.main.banner-mode=off"))
            .hasStackTraceContaining("dzdcache.cluster-transport is websocket, but this application runs no servlet web server with WebSocket support");
    }

    @Test void anUnknownClusterTransportIsRefused() {
        assertThatThrownBy(() -> new SpringApplicationBuilder(Students.class).web(WebApplicationType.NONE).run("--dzdcache.discovery.mode=static",
                "--dzdcache.cluster-token=secret", "--dzdcache.cluster-transport=quic", "--logging.level.root=OFF", "--spring.main.banner-mode=off"))
            .hasStackTraceContaining("cluster-transport must be tcp, http2 or websocket");
    }

    @Test void anUnknownClusterEncodingIsRefused() {
        assertThatThrownBy(() -> new SpringApplicationBuilder(Students.class).web(WebApplicationType.NONE).run("--dzdcache.discovery.mode=static",
                "--dzdcache.cluster-token=secret", "--dzdcache.cluster-encoding=avro", "--logging.level.root=OFF", "--spring.main.banner-mode=off"))
            .hasStackTraceContaining("cluster-encoding must be json or smile");
    }

    @Test void writesResumeThroughANewMasterWhenTheMasterStops() throws Exception {
        var started = start(Students.class, Students.class, Students.class);
        ConfigurableApplicationContext master = started.stream().filter(node -> cluster(node).role() == ClusterCoordinator.Role.MASTER)
            .findFirst().orElseThrow();
        List<ConfigurableApplicationContext> others = started.stream().filter(node -> node != master).toList();
        await(() -> others.stream().allMatch(node -> node.getBean(EngineHolder.class).engine().contains("students", 1)));
        students(others.get(0)).put(new Student(1, "Anna", "Oslo", 22));
        master.close();
        await(() -> others.stream().anyMatch(node -> cluster(node).role() == ClusterCoordinator.Role.MASTER));
        assertThat(students(others.get(1)).put(new Student(2, "Anders", "Bergen", 31))).isEqualTo(1);
        await(() -> others.stream().allMatch(node -> node.getBean(DistributedCache.class).current()
            && students(node).get(1L) != null && students(node).get(2L) != null));
    }

    @Test void aPartitionedMasterStopsServingAndTheOthersServeEveryAcknowledgedWrite() throws Exception {
        var started = start(PartitionableStudents.class, PartitionableStudents.class, PartitionableStudents.class);
        await(() -> started.stream().allMatch(node -> node.getBean(EngineHolder.class).engine().contains("students", 1)));
        ConfigurableApplicationContext master = started.stream().filter(node -> cluster(node).role() == ClusterCoordinator.Role.MASTER)
            .findFirst().orElseThrow();
        List<ConfigurableApplicationContext> others = started.stream().filter(node -> node != master).toList();
        students(master).put(new Student(1, "Anna", "Oslo", 22));
        cutOff.add(master.getBean(ServiceProperties.class).getClusterPort());
        await(() -> !master.getBean(DistributedCache.class).current());
        assertThatThrownBy(() -> students(master).get(1L)).isInstanceOf(CacheException.class)
            .satisfies(error -> assertThat(((CacheException) error).status()).isEqualTo(503));
        assertThat(master.getBean(io.micrometer.core.instrument.MeterRegistry.class).find("dzdcache.reads.refused").counters())
            .anySatisfy(counter -> assertThat(counter.count()).isPositive());
        await(() -> others.stream().anyMatch(node -> cluster(node).role() == ClusterCoordinator.Role.MASTER));
        assertThat(students(others.get(0)).put(new Student(1, "Anna", "Bergen", 22))).isEqualTo(2);
        for (ConfigurableApplicationContext node : others) {
            await(() -> node.getBean(DistributedCache.class).current());
            assertThat(students(node).get(1L).city()).as("an acknowledged write is visible at once").isEqualTo("Bergen");
        }
        cutOff.clear();
        await(() -> master.getBean(DistributedCache.class).current());
        assertThat(students(master).get(1L).city()).isEqualTo("Bergen");
    }

    @Test void readsDuringAFailoverWaitForTheNewMasterInsteadOfFailing() throws Exception {
        var started = start(PartitionableStudents.class, PartitionableStudents.class, PartitionableStudents.class);
        await(() -> started.stream().allMatch(node -> node.getBean(EngineHolder.class).engine().contains("students", 1)));
        ConfigurableApplicationContext master = started.stream().filter(node -> cluster(node).role() == ClusterCoordinator.Role.MASTER)
            .findFirst().orElseThrow();
        ConfigurableApplicationContext reader = started.stream().filter(node -> node != master).findFirst().orElseThrow();
        students(master).put(new Student(1, "Anna", "Oslo", 22));
        await(() -> reader.getBean(DistributedCache.class).current());
        cutOff.add(master.getBean(ServiceProperties.class).getClusterPort());
        long until = System.nanoTime() + Duration.ofSeconds(4).toNanos();
        int answered = 0;
        while (System.nanoTime() < until) {
            assertThat(students(reader).get(1L)).isEqualTo(new Student(1, "Anna", "Oslo", 22));
            answered++;
            Thread.sleep(25);
        }
        assertThat(cluster(reader).role() == ClusterCoordinator.Role.MASTER
            || started.stream().anyMatch(node -> node != master && cluster(node).role() == ClusterCoordinator.Role.MASTER)).isTrue();
        assertThat(answered).isPositive();
    }

    @Test void aClusterGrowsAndShrinksLiveAndKeepsItsData() throws Exception {
        var started = start(Students.class, Students.class);
        await(() -> started.stream().allMatch(node -> node.getBean(EngineHolder.class).engine().contains("students", 1)));
        students(started.get(0)).put(new Student(1, "Anna", "Oslo", 22));
        int port;
        try (var socket = new ServerSocket(0)) { port = socket.getLocalPort(); }
        List<String> args = new ArrayList<>(List.of("--dzdcache.discovery.mode=static", "--dzdcache.discovery.node-id=node-x",
            "--dzdcache.cluster-port=" + port, "--dzdcache.discovery.self-url=http://localhost:" + port,
            "--dzdcache.cluster-token=secret", "--logging.level.root=OFF", "--spring.main.banner-mode=off"));
        int peer = 0;
        for (ConfigurableApplicationContext node : started)
            args.add("--dzdcache.discovery.static-peers[" + peer++ + "]=http://localhost:" + node.getBean(ServiceProperties.class).getClusterPort());
        ConfigurableApplicationContext third = new SpringApplicationBuilder(Students.class).web(WebApplicationType.NONE).run(args.toArray(String[]::new));
        nodes.add(third);
        await(() -> cluster(third).startupProblem() != null);
        assertThat(cluster(third).startupProblem()).contains("2 members", "resize the cluster first");

        started.get(1).getBean(DistributedCache.class).resize(3);
        await(() -> third.getBean(DistributedCache.class).current());
        assertThat(cluster(started.get(0)).view().size()).isEqualTo(3);
        assertThat(cluster(started.get(0)).view().members()).hasSize(3);
        assertThat(students(third).get(1L)).isEqualTo(new Student(1, "Anna", "Oslo", 22));
        assertThat(cluster(third).sizeWarning()).isNull();
        assertThat(cluster(started.get(0)).sizeWarning()).contains("cluster-size is 2", "has 3");

        third.getBean(DistributedCache.class).resize(2, "node-x");
        await(() -> !third.getBean(DistributedCache.class).ready());
        assertThat(cluster(started.get(0)).view().size()).isEqualTo(2);
        assertThat(cluster(started.get(0)).view().members()).extracting(Member::id).containsExactlyInAnyOrder("node-0", "node-1");
        assertThat(students(started.get(1)).put(new Student(2, "Berit", "Bergen", 30))).isEqualTo(1);
        await(() -> students(started.get(0)).get(2L) != null);
        assertThatThrownBy(() -> started.get(0).getBean(DistributedCache.class).resize(1)).isInstanceOf(CacheException.class)
            .hasMessageContaining("name the members that leave");
    }

    @Test void withAWitnessTwoInstancesSurviveABrokenLinkWithOneMasterAndThenTheMasterStopping() throws Exception {
        int witnessPort;
        try (var socket = new ServerSocket(0)) { witnessPort = socket.getLocalPort(); }
        ConfigurableApplicationContext witness = new SpringApplicationBuilder(Students.class).web(WebApplicationType.NONE).run(
            "--dzdcache.discovery.mode=witness", "--dzdcache.cluster-port=" + witnessPort, "--dzdcache.cluster-token=secret",
            "--logging.level.root=WARN", "--spring.main.banner-mode=off");
        nodes.add(witness);
        assertThat(witness.getBean(DzdCacheHealthIndicator.class).health().getDetails()).containsEntry("role", "WITNESS");
        var started = start("http", List.of("--dzdcache.witness-url=http://localhost:" + witnessPort),
            PartitionableStudents.class, PartitionableStudents.class);
        await(() -> started.stream().allMatch(node -> node.getBean(EngineHolder.class).engine().contains("students", 1)));
        ConfigurableApplicationContext master = started.stream().filter(node -> cluster(node).role() == ClusterCoordinator.Role.MASTER)
            .findFirst().orElseThrow();
        ConfigurableApplicationContext other = started.stream().filter(node -> node != master).findFirst().orElseThrow();
        students(master).put(new Student(1, "Anna", "Oslo", 22));
        cutLinks.add(link(master.getBean(ServiceProperties.class).getClusterPort(), other.getBean(ServiceProperties.class).getClusterPort()));
        await(() -> !other.getBean(DistributedCache.class).current());
        Thread.sleep(4_000);
        assertThat(cluster(master).role()).isEqualTo(ClusterCoordinator.Role.MASTER);
        assertThat(cluster(other).role()).as("the witness keeps the younger from taking over").isNotEqualTo(ClusterCoordinator.Role.MASTER);
        assertThat(students(master).put(new Student(2, "Berit", "Bergen", 30))).isEqualTo(1);
        cutLinks.clear();
        await(() -> other.getBean(DistributedCache.class).current() && students(other).get(2L) != null);

        master.close();
        await(() -> cluster(other).role() == ClusterCoordinator.Role.MASTER);
        assertThat(students(other).put(new Student(3, "Cid", "Oslo", 40))).isEqualTo(1);
        assertThat(students(other).get(2L)).as("capacity 2 keeps the two most recently used").isEqualTo(new Student(2, "Berit", "Bergen", 30));
    }

    @Test void aTypeOnlySomeInstancesDeclareIsUnavailableOnTheOthers() throws Exception {
        var started = start(StudentsAndCourses.class, Students.class, Students.class);
        await(() -> started.stream().allMatch(node -> node.getBean(EngineHolder.class).engine().descriptors().size() == 2));
        assertThat(started.get(1).getBean(EngineHolder.class).engine().unavailableTypes()).containsExactly("courses/1");
        ClusterCache<Course, String> courses = started.get(0).getBean(DistributedCache.class).cache("courses");
        assertThat(cluster(started.get(0)).role()).as("the first node founds the cluster").isEqualTo(ClusterCoordinator.Role.MASTER);
        assertThat(courses.put(new Course("CS101", "Introduction"))).isEqualTo(1);
        assertThat(courses.get("CS101")).isEqualTo(new Course("CS101", "Introduction"));
        started.get(0).close();
        var students = students(started.get(1));
        await(() -> cluster(started.get(1)).role() == ClusterCoordinator.Role.MASTER || cluster(started.get(2)).role() == ClusterCoordinator.Role.MASTER);
        assertThat(students.put(new Student(1, "Anna", "Oslo", 22))).isEqualTo(1);
        assertThatThrownBy(() -> started.get(1).getBean(DistributedCache.class).cache("courses")).isInstanceOf(IllegalArgumentException.class);
    }
}
