package io.github.dzdcache.cache;

import java.util.function.Supplier;

public final class EngineHolder {
    private final Supplier<CacheEngine> factory;
    private volatile CacheEngine engine;

    public EngineHolder(Supplier<CacheEngine> factory) {
        this.factory = factory;
        this.engine = factory.get();
    }

    private volatile int partitions = 1;

    public void partitions(int count) {
        partitions = count;
        engine.partitions(count);
    }

    public CacheEngine engine() { return engine; }
    public CacheEngine fresh() {
        CacheEngine created = factory.get();
        created.partitions(partitions);
        return created;
    }
    public void replace(CacheEngine next) { engine = next; }
}
