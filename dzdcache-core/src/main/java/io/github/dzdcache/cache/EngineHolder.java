package io.github.dzdcache.cache;

import java.util.function.Supplier;

public final class EngineHolder {
    private final Supplier<CacheEngine> factory;
    private volatile CacheEngine engine;

    public EngineHolder(Supplier<CacheEngine> factory) {
        this.factory = factory;
        this.engine = factory.get();
    }

    private volatile Change.Sink sink;

    public void changes(Change.Sink sink) {
        this.sink = sink;
        engine.changes(sink);
    }

    public CacheEngine engine() { return engine; }
    public CacheEngine fresh() {
        CacheEngine created = factory.get();
        created.changes(sink);
        return created;
    }
    public void replace(CacheEngine next) { engine = next; }
}
