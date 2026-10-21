package io.github.dzdcache.spring;

import io.github.dzdcache.cache.CacheException;
import io.github.dzdcache.cache.EngineHolder;
import io.github.dzdcache.cache.EntryMutation;
import io.github.dzdcache.cache.PutRequest;
import io.github.dzdcache.cluster.ClusterCoordinator;
import io.github.zdcache.Getter;
import io.github.zdcache.internal.Getters;
import java.util.Collection;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Predicate;
import tools.jackson.databind.ObjectMapper;

public final class ClusterCache<T, ID> {
    private final CacheType<T, ID> type;
    private final EngineHolder engines;
    private final ClusterCoordinator cluster;
    private final ObjectMapper values;

    ClusterCache(CacheType<T, ID> type, EngineHolder engines, ClusterCoordinator cluster, ObjectMapper values) {
        this.type = type; this.engines = engines; this.cluster = cluster; this.values = values;
    }

    public CacheType<T, ID> type() { return type; }

    public T get(ID id) {
        cluster.awaitCurrent();
        return type.javaClass().cast(engines.engine().find(type.name(), type.version(), id));
    }

    public List<T> search(String dimension, Object... query) { return search((Integer) null, dimension, query); }

    public List<T> search(Getter<T, ?> dimension, Object... query) { return search((Integer) null, Getters.name(dimension), query); }

    public List<T> search(Integer limit, String dimension, Object... query) {
        cluster.awaitCurrent();
        return engines.engine().search(type.name(), type.version(), dimension, query, limit).stream().map(type.javaClass()::cast).toList();
    }

    public List<T> search(String dimension, Predicate<? super T> filter, Object... query) {
        cluster.awaitCurrent();
        return engines.engine().search(type.name(), type.version(), dimension, query, null, typed(filter)).stream().map(type.javaClass()::cast).toList();
    }

    public List<T> scan(Predicate<? super T> filter) {
        cluster.awaitCurrent();
        return engines.engine().scan(type.name(), type.version(), null, typed(filter)).stream().map(type.javaClass()::cast).toList();
    }

    public long count(String dimension, Object... query) {
        cluster.awaitCurrent();
        return engines.engine().count(type.name(), type.version(), dimension, query, null, false);
    }

    public long count(String dimension, Predicate<? super T> filter, Object... query) {
        cluster.awaitCurrent();
        return engines.engine().count(type.name(), type.version(), dimension, query, typed(filter), false);
    }

    public <K> Map<K, Long> countBy(Function<? super T, ? extends K> key, String dimension, Object... query) {
        cluster.awaitCurrent();
        Map<Object, Long> counts = engines.engine().countBy(type.name(), type.version(), dimension, query, null, false,
            root -> { K value = key.apply(type.javaClass().cast(root)); return value == null ? List.of() : List.of(value); });
        Map<K, Long> typedCounts = new LinkedHashMap<>();
        counts.forEach((value, count) -> {
            @SuppressWarnings("unchecked") K k = (K) value;
            typedCounts.put(k, count);
        });
        return typedCounts;
    }

    private Predicate<Object> typed(Predicate<? super T> filter) {
        Class<T> javaClass = type.javaClass();
        return root -> filter.test(javaClass.cast(root));
    }

    public long put(T value) { return put(value, 0); }

    public long put(T value, long revision) { return revisionOf(cluster.put(List.of(request(value, revision))).getFirst()); }

    public CompletableFuture<Long> putAsync(T value) { return putAsync(value, 0); }

    public CompletableFuture<Long> putAsync(T value, long revision) {
        return cluster.putAsync(List.of(request(value, revision))).thenApply(outcomes -> revisionOf(outcomes.getFirst()));
    }

    public List<ClusterCoordinator.Outcome> putAll(Collection<? extends T> entries) {
        return cluster.put(entries.stream().map(value -> request(value, 0)).toList());
    }

    public long invalidate(ID id) { return invalidate(id, 0); }

    public long invalidate(ID id, long revision) { return change(EntryMutation.Operation.INVALIDATE, id, revision); }

    public void evict(ID id) { change(EntryMutation.Operation.EVICT, id, 0); }

    private long change(EntryMutation.Operation operation, ID id, long revision) {
        return revisionOf(cluster.change(new EntryMutation(operation, type.name(), type.version(), values.valueToTree(id), null, revision, null)));
    }

    private PutRequest request(T value, long revision) {
        Long writtenAt = type.ttl() == null ? null : System.currentTimeMillis();
        return new PutRequest(type.name(), type.version(), revision, values.valueToTree(value), writtenAt, null);
    }

    private static long revisionOf(ClusterCoordinator.Outcome outcome) {
        if (outcome.status() != 200) throw new CacheException(outcome.status(), outcome.error());
        return outcome.mutation() == null ? 0 : outcome.mutation().revision();
    }
}
