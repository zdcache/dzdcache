package io.github.dzdcache.benchmark;

import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.*;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Threads(4)
public class KeyValueBenchmark {
    private static final int ORDER_MASK = (1 << 16) - 1;

    @Param({"dzdcache-embedded", "dzdcache-server", "hazelcast", "ignite", "infinispan", "redis-stack"})
    String subject;

    @Param({"1000", "10000", "50000"})
    int size;

    private DistributedSubject cache;
    private Person[][] versions;
    private int[] order;

    @State(Scope.Thread)
    public static class Cursor {
        private int next;
        int step() { return next++; }
    }

    @Setup(Level.Trial)
    public void setUp() {
        Person[] people = Dataset.people(size, 42);
        versions = new Person[][] {people, Dataset.changed(people, 43)};
        order = Dataset.accessOrder(size, ORDER_MASK + 1, 44);
        cache = Subjects.create(subject, size);
        cache.load(people);
    }

    @TearDown(Level.Trial)
    public void tearDown() { cache.close(); }

    @Benchmark
    public Person get(Cursor cursor) { return cache.get(order[cursor.step() & ORDER_MASK]); }

    @Benchmark
    public void put(Cursor cursor) {
        int i = cursor.step();
        cache.put(versions[(i >>> 16) & 1][order[i & ORDER_MASK]]);
    }
}
