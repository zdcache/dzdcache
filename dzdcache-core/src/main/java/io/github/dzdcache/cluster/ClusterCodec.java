package io.github.dzdcache.cluster;

import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.dataformat.smile.SmileMapper;

public final class ClusterCodec {
    public static final String HEADER = "X-Dzdcache-Encoding";
    static final ObjectMapper GREETING = JsonMapper.builder().build();

    private ClusterCodec() { }

    public static ObjectMapper of(String encoding, ObjectMapper json) {
        return switch (encoding) {
            case "json" -> json;
            case "smile" -> SmileMapper.builder().enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS).build();
            default -> throw new IllegalArgumentException("cluster-encoding must be json or smile");
        };
    }

    public static String contentType(String encoding) { return encoding.equals("smile") ? "application/x-jackson-smile" : "application/json"; }
}
