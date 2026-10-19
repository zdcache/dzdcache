package io.github.dzdcache.spring;

import io.github.dzdcache.cache.EngineHolder;
import io.github.dzdcache.cluster.ClusterCoordinator;
import io.github.zdcache.IdentityDimension;
import io.github.zdcache.SearchSpecification;
import io.github.zdcache.ValueRange;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
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

    private final List<ConfigurableApplicationContext> nodes = new ArrayList<>();

    @AfterEach void stop() { nodes.forEach(ConfigurableApplicationContext::close); }

    private List<ConfigurableApplicationContext> start(Class<?>... configurations) throws Exception {
        int[] ports = new int[configurations.length];
        for (int i = 0; i < ports.length; i++) try (var socket = new ServerSocket(0)) { ports[i] = socket.getLocalPort(); }
        List<ConfigurableApplicationContext> started = new ArrayList<>();
        for (int i = 0; i < ports.length; i++) {
            List<String> args = new ArrayList<>(List.of("--dzdcache.discovery.mode=static", "--dzdcache.discovery.node-id=node-" + i,
                "--dzdcache.cluster-port=" + ports[i], "--dzdcache.discovery.self-url=http://localhost:" + ports[i],
                "--dzdcache.cluster-token=secret", "--logging.level.root=WARN", "--spring.main.banner-mode=off"));
            int peer = 0;
            for (int j = 0; j < ports.length; j++)
                if (j != i) args.add("--dzdcache.discovery.static-peers[" + peer++ + "]=http://localhost:" + ports[j]);
            ConfigurableApplicationContext node = new SpringApplicationBuilder(configurations[i]).web(WebApplicationType.NONE)
                .run(args.toArray(String[]::new));
            nodes.add(node);
            started.add(node);
            Thread.sleep(20);
        }
        await(() -> started.stream().allMatch(node -> node.getBean(DistributedCache.class).ready()));
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
        await(() -> others.stream().allMatch(node -> students(node).get(1L) != null && students(node).get(2L) != null));
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
