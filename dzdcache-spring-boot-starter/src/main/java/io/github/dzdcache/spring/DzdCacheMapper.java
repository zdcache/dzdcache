package io.github.dzdcache.spring;

import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.json.JsonMapper;

public record DzdCacheMapper(JsonMapper mapper) {
    public static DzdCacheMapper create() {
        return new DzdCacheMapper(JsonMapper.builder().enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS).build());
    }
}
