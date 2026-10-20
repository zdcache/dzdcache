package io.github.dzdcache.benchmark;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.IntFunction;

public final class Subjects {
    public static final String DZDCACHE_EMBEDDED = "dzdcache-embedded";
    public static final String DZDCACHE_SERVER = "dzdcache-server";
    public static final String IGNITE = "ignite";
    public static final String REDIS_STACK = "redis-stack";

    private static final Map<String, IntFunction<DistributedSubject>> FACTORIES = new LinkedHashMap<>();
    static {
        FACTORIES.put(DZDCACHE_EMBEDDED, DzdcacheEmbeddedSubject::new);
        FACTORIES.put(DZDCACHE_SERVER, DzdcacheServerSubject::new);
        FACTORIES.put("hazelcast", HazelcastSubject::new);
        FACTORIES.put(IGNITE, IgniteSubject::new);
        FACTORIES.put("infinispan", InfinispanSubject::new);
        FACTORIES.put(REDIS_STACK, RedisStackSubject::new);
    }

    private Subjects() { }

    public static List<String> names() { return List.copyOf(FACTORIES.keySet()); }

    public static boolean available(String name) { return !inDocker(name) || RedisStackSubject.available(); }

    public static boolean inDocker(String name) { return name.equals(REDIS_STACK) || name.equals(DZDCACHE_SERVER); }

    public static DistributedSubject create(String name, int capacity) {
        IntFunction<DistributedSubject> factory = FACTORIES.get(name);
        if (factory == null) throw new IllegalArgumentException("Unknown subject " + name + "; choose one of " + names());
        return factory.apply(capacity);
    }
}
