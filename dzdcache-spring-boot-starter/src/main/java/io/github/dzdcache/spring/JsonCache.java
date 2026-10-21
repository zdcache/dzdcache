package io.github.dzdcache.spring;

import io.github.dzdcache.cache.CacheEngine;
import io.github.dzdcache.cache.EngineHolder;
import io.github.dzdcache.cluster.ClusterCoordinator;
import java.util.ArrayList;
import java.util.List;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

public final class JsonCache {
    private final String specification;
    private final int version;
    private final EngineHolder engines;
    private final ClusterCoordinator cluster;
    private final ObjectMapper mapper;

    JsonCache(String specification, int version, EngineHolder engines, ClusterCoordinator cluster, ObjectMapper mapper) {
        this.specification = specification; this.version = version; this.engines = engines; this.cluster = cluster; this.mapper = mapper;
    }

    public JsonNode get(Object identity) {
        cluster.awaitCurrent();
        return engines.engine().get(specification, version, mapper.valueToTree(identity));
    }

    public List<JsonNode> search(String dimension, Object value) { return search(dimension, value, null); }

    public List<JsonNode> search(String dimension, Object value, JsonNode filter) {
        cluster.awaitCurrent();
        CacheEngine engine = engines.engine();
        CacheEngine.Selection selection = engine.select(specification, version, dimension, mapper.valueToTree(value), null, null, filter, false);
        List<JsonNode> found = new ArrayList<>();
        for (JsonNode root : mapper.readTree(engine.searchJson(specification, version, selection, null, null).json())) found.add(root);
        return found;
    }

    public long count(String dimension, Object value, JsonNode filter) {
        cluster.awaitCurrent();
        CacheEngine engine = engines.engine();
        return engine.countJson(specification, version, engine.select(specification, version, dimension, mapper.valueToTree(value), null, null, filter, false));
    }
}
