package io.github.dzdcache.benchmark;

import java.util.List;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.*;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Threads(4)
public class SearchBenchmark {
    @Param({"dzdcache-embedded", "dzdcache-server", "hazelcast", "ignite", "infinispan", "redis-stack"})
    String subject;

    @Param({"1000", "10000", "50000"})
    int size;

    private DistributedSubject cache;
    private Queries queries;

    @State(Scope.Thread)
    public static class Cursor {
        private int next;
        int query() { return next++ & (Queries.COUNT - 1); }
    }

    @Setup(Level.Trial)
    public void setUp() {
        Person[] people = Dataset.people(size, 42);
        queries = new Queries(people, 7);
        cache = Subjects.create(subject, size);
        cache.load(people);
    }

    @TearDown(Level.Trial)
    public void tearDown() { cache.close(); }

    @Benchmark
    public List<Person> exactCity(Cursor cursor) { return cache.byCity(queries.cities[cursor.query()]); }

    @Benchmark
    public List<Person> prefixName(Cursor cursor) { return cache.byNamePrefix(queries.namePrefixes[cursor.query()]); }

    @Benchmark
    public List<Person> rangeAge(Cursor cursor) {
        int q = cursor.query();
        return cache.byAge(queries.ageFrom[q], queries.ageTo[q]);
    }

    @Benchmark
    public List<Person> compoundCityAge(Cursor cursor) {
        int q = cursor.query();
        return cache.byCityAndAge(queries.cities[q], queries.ageFrom[q], queries.ageTo[q]);
    }

    @Benchmark
    public List<Person> nestedTag(Cursor cursor) { return cache.byTag(queries.tags[cursor.query()]); }
}
