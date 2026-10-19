package io.github.dzdcache.cache;

import io.github.zdcache.SearchSpecification;
import java.util.List;
import tools.jackson.core.JsonGenerator;
import tools.jackson.databind.JsonNode;

public interface RootType<T> {
    record Query(String dimension, Object[] values) { }

    Class<T> javaClass();
    SearchSpecification<T, Object> specification();
    Object identity(T root);
    Object readIdentity(JsonNode identity);
    JsonNode writeIdentity(Object identity);
    T read(JsonNode value);
    JsonNode write(T root);

    default Query query(String dimension, JsonNode value, JsonNode from, JsonNode to) {
        throw new IllegalArgumentException("Searches on " + javaClass().getSimpleName() + " take Java values");
    }

    default void render(JsonGenerator out, T root, List<String> fields) {
        if (fields != null && !fields.isEmpty()) throw new IllegalArgumentException("Field selection is not supported for this type");
        out.writeTree(write(root));
    }
}
