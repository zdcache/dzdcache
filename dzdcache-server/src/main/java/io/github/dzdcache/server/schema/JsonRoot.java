package io.github.dzdcache.server.schema;

public final class JsonRoot {
    private final Object identity;
    private final byte[] json;
    private final Object[] values;

    JsonRoot(Object identity, byte[] json, Object[] values) { this.identity = identity; this.json = json; this.values = values; }

    public Object identity() { return identity; }
    byte[] json() { return json; }
    Object value(int slot) { return values[slot]; }
}
