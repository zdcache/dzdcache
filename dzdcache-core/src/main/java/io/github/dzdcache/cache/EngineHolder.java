package io.github.dzdcache.cache;

import java.util.function.Supplier;

public final class EngineHolder {
    private final Supplier<CacheEngine> factory;
    private volatile CacheEngine engine;

    public EngineHolder(Supplier<CacheEngine> factory) {
        this.factory = factory;
        this.engine = factory.get();
    }

    public CacheEngine engine() { return engine; }
    public CacheEngine fresh() { return factory.get(); }
    public void replace(CacheEngine next) { engine = next; }
}
