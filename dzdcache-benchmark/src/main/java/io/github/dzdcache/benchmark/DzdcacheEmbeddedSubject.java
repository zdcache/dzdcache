package io.github.dzdcache.benchmark;

import io.github.dzdcache.cluster.ClusterCoordinator;
import io.github.dzdcache.config.ServiceProperties;
import io.github.dzdcache.spring.CacheType;
import io.github.dzdcache.spring.ClusterCache;
import io.github.dzdcache.spring.DistributedCache;
import io.github.zdcache.IdentityDimension;
import io.github.zdcache.SearchSpecification;
import io.github.zdcache.ValueRange;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;

public final class DzdcacheEmbeddedSubject implements DistributedSubject, TimedWrites {
    @SpringBootConfiguration @EnableAutoConfiguration
    public static class Node {
        @Bean CacheType<Person, Long> people(@Value("${benchmark.capacity}") int capacity) {
            return CacheType.of("people", 1, Long.class, specification()).capacity(capacity);
        }
        @Bean MeterRegistry meters() { return new SimpleMeterRegistry(); }
    }

    static SearchSpecification<Person, Long> specification() {
        return SearchSpecification.builder(IdentityDimension.of(Person.class, "id", Person::id))
            .exact(Person::city).prefix(Person::name).range(Person::age)
            .compound("cityAge", parts -> parts.exact(Person::city).range(Person::age))
            .exactEach(Person::tags).build();
    }

    private final List<ConfigurableApplicationContext> nodes = new ArrayList<>();
    private final ClusterCache<Person, Long> first;
    private final ClusterCache<Person, Long> second;

    public DzdcacheEmbeddedSubject(int capacity) {
        int[] ports = Ports.free(3);
        for (int i = 0; i < 3; i++) {
            List<String> args = new ArrayList<>(List.of("--dzdcache.discovery.mode=static", "--dzdcache.discovery.node-id=node-" + i,
                "--dzdcache.cluster-port=" + ports[i], "--dzdcache.discovery.self-url=http://127.0.0.1:" + ports[i],
                "--dzdcache.cluster-token=benchmark", "--dzdcache.max-search-results=1000000", "--benchmark.capacity=" + capacity,
                "--logging.level.root=WARN", "--spring.main.banner-mode=off"));
            int peer = 0;
            for (int j = 0; j < 3; j++) if (j != i) args.add("--dzdcache.discovery.static-peers[" + peer++ + "]=http://127.0.0.1:" + ports[j]);
            nodes.add(new SpringApplicationBuilder(Node.class).web(WebApplicationType.NONE).run(args.toArray(String[]::new)));
        }
        Waits.until("the dzdcache nodes serve fresh reads", () -> nodes.stream().allMatch(node -> {
            DistributedCache cache = node.getBean(DistributedCache.class);
            return cache.current() && node.getBean(ClusterCoordinator.class).clusterSize() == 3;
        }));
        Waits.until("every node knows the people type", () -> nodes.stream().allMatch(node -> {
            try { cache(node).get(-1L); return true; } catch (RuntimeException e) { return false; }
        }));
        first = cache(nodes.get(0));
        second = cache(nodes.get(1));
    }

    private static ClusterCache<Person, Long> cache(ConfigurableApplicationContext node) {
        return node.getBean(DistributedCache.class).cache(Person.class, Long.class);
    }

    @Override public String settings() {
        return "3 Spring Boot starter instances in one JVM over HTTP cluster ports; exact city, prefix name, range age, "
            + "compound city+age and exact-each tags; reads are local on instance 2 and wait for a read lease";
    }
    @Override public void load(Person[] people) {
        for (int start = 0; start < people.length; start += 1_000) {
            var outcomes = first.putAll(Arrays.asList(people).subList(start, Math.min(people.length, start + 1_000)));
            if (outcomes.stream().anyMatch(outcome -> outcome.status() != 200)) throw new IllegalStateException("Load failed: " + outcomes);
        }
    }
    @Override public void putViaFirst(Person person) { first.put(person); }
    @Override public void put(Person person) { second.put(person); }
    @Override public Person get(long id) { return second.get(id); }
    @Override public List<Person> byCity(String city) { return second.search("city", city); }
    @Override public List<Person> byNamePrefix(String prefix) { return second.search("name", prefix); }
    @Override public List<Person> byAge(int from, int to) { return second.search("age", ValueRange.between(from, to)); }
    @Override public List<Person> byCityAndAge(String city, int from, int to) {
        return second.search("cityAge", city, ValueRange.between(from, to));
    }
    @Override public List<Person> byTag(String tag) { return second.search("tags", tag); }
    @Override public List<Map<String, Totals>> timersPerNode() {
        return nodes.stream().map(node -> {
            Map<String, Totals> timers = new TreeMap<>();
            for (Meter meter : node.getBean(MeterRegistry.class).getMeters()) {
                if (!(meter instanceof Timer timer) || !timer.getId().getName().startsWith("dzdcache.")) continue;
                Map<String, String> tags = timer.getId().getTags().stream().collect(Collectors.toMap(Tag::getKey, Tag::getValue));
                timers.put(TimedWrites.key(timer.getId().getName(), tags), new Totals(timer.count(), timer.totalTime(TimeUnit.SECONDS)));
            }
            return timers;
        }).toList();
    }
    @Override public int masterNode() {
        for (int i = 0; i < nodes.size(); i++) if (nodes.get(i).getBean(ClusterCoordinator.class).role() == ClusterCoordinator.Role.MASTER) return i;
        return -1;
    }
    @Override public void close() { nodes.forEach(ConfigurableApplicationContext::close); }
}
