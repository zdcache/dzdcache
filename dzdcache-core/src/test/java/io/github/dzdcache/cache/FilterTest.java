package io.github.dzdcache.cache;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FilterTest {
    private final JsonMapper mapper = JsonMapper.builder().build();

    private static final Filter.Paths MAPS = new Filter.Paths() {
        @Override public Function<Object, List<Object>> values(String path, String type) {
            String[] segments = path.substring(1).split("/");
            return root -> { List<Object> found = new ArrayList<>(); walk(root, segments, 0, found); return found; };
        }
        @Override public Object literal(String path, JsonNode value, String type) {
            return value.isString() ? value.asString() : value.isNumber() ? value.decimalValue() : value.asBoolean();
        }
    };

    private static void walk(Object node, String[] segments, int at, List<Object> found) {
        if (node == null) return;
        if (at == segments.length) { found.add(node); return; }
        if (segments[at].equals("*")) { if (node instanceof List<?> list) for (Object item : list) walk(item, segments, at + 1, found); }
        else if (node instanceof Map<?, ?> map) walk(map.get(segments[at]), segments, at + 1, found);
    }

    private Predicate<Object> filter(String json) { return Filter.parse(mapper.readTree(json)).compile(MAPS); }

    private static final Map<String, Object> ANNA = Map.of("name", "Anna", "age", 31L, "tags", List.of("chess", "piano"), "address", Map.of("zip", "0301"));
    private static final Map<String, Object> BO = Map.of("name", "Bo", "age", new BigDecimal("19.0"), "tags", List.of("football"));

    @Test void comparesNumbersWhateverTheirJavaType() {
        assertThat(filter("{\"path\":\"/age\",\"op\":\"gte\",\"value\":30}")).accepts(ANNA).rejects(BO);
        assertThat(filter("{\"path\":\"/age\",\"op\":\"eq\",\"value\":19}")).accepts(BO).rejects(ANNA);
        assertThat(filter("{\"path\":\"/age\",\"op\":\"lt\",\"value\":19.5}")).accepts(BO);
        assertThat(filter("{\"path\":\"/age\",\"op\":\"in\",\"value\":[31,40]}")).accepts(ANNA).rejects(BO);
    }

    @Test void collectionsMatchWhenAnyElementMatches() {
        assertThat(filter("{\"path\":\"/tags/*\",\"op\":\"eq\",\"value\":\"chess\"}")).accepts(ANNA).rejects(BO);
        assertThat(filter("{\"path\":\"/tags/*\",\"op\":\"ne\",\"value\":\"chess\"}")).accepts(BO).rejects(ANNA);
        assertThat(filter("{\"path\":\"/tags/*\",\"op\":\"prefix\",\"value\":\"foot\"}")).accepts(BO).rejects(ANNA);
    }

    @Test void combinesConditionsAndChecksPresence() {
        assertThat(filter("{\"all\":[{\"path\":\"/age\",\"op\":\"gt\",\"value\":20},{\"not\":{\"path\":\"/address/zip\",\"op\":\"prefix\",\"value\":\"03\"}}]}"))
            .rejects(ANNA).rejects(BO);
        assertThat(filter("{\"any\":[{\"path\":\"/name\",\"op\":\"eq\",\"value\":\"Bo\"},{\"path\":\"/address/zip\",\"op\":\"exists\"}]}"))
            .accepts(ANNA).accepts(BO);
        assertThat(filter("{\"path\":\"/address\",\"op\":\"exists\"}")).accepts(ANNA).rejects(BO);
    }

    @Test void refusesMalformedFiltersWithAReason() {
        assertThatThrownBy(() -> filter("{\"path\":\"/age\",\"op\":\"between\",\"value\":1}")).hasMessageContaining("Unknown filter op 'between'");
        assertThatThrownBy(() -> filter("{\"path\":\"/age\",\"op\":\"gt\"}")).hasMessageContaining("'gt' needs a value");
        assertThatThrownBy(() -> filter("{\"path\":\"/age\",\"op\":\"exists\",\"value\":1}")).hasMessageContaining("'exists' takes no value");
        assertThatThrownBy(() -> filter("{\"path\":\"/age\",\"op\":\"in\",\"value\":3}")).hasMessageContaining("'in' takes an array");
        assertThatThrownBy(() -> filter("{\"all\":[]}")).hasMessageContaining("non-empty array");
        assertThatThrownBy(() -> filter("{\"path\":\"/flag\",\"op\":\"gt\",\"value\":true}")).hasMessageContaining("comparable");
        assertThatThrownBy(() -> filter("{\"path\":\"/age\",\"op\":\"prefix\",\"value\":3}")).hasMessageContaining("'prefix' compares strings");
        StringBuilder many = new StringBuilder("{\"all\":[");
        for (int i = 0; i < 65; i++) many.append(i == 0 ? "" : ",").append("{\"path\":\"/age\",\"op\":\"exists\"}");
        assertThatThrownBy(() -> filter(many + "]}")).hasMessageContaining("at most 64 conditions");
    }
}
