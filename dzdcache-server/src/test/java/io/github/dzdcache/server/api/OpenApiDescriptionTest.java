package io.github.dzdcache.server.api;

import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.yaml.snakeyaml.Yaml;

import static org.assertj.core.api.Assertions.assertThat;

class OpenApiDescriptionTest {
    private static final int MAX_DESCRIPTION = 499;

    @Test void everyDescriptionFitsTheDeveloperPortalLimit() throws Exception {
        Path contract = Files.exists(Path.of("openapi.yaml")) ? Path.of("openapi.yaml") : Path.of("..", "openapi.yaml");
        Object document;
        try (Reader reader = Files.newBufferedReader(contract)) { document = new Yaml().load(reader); }
        Map<String, String> descriptions = new LinkedHashMap<>();
        collect(document, "", descriptions);
        assertThat(descriptions).isNotEmpty();
        descriptions.forEach((path, text) -> assertThat(text.length()).as(path).isLessThanOrEqualTo(MAX_DESCRIPTION));
    }

    private static void collect(Object node, String path, Map<String, String> descriptions) {
        if (node instanceof Map<?, ?> map) {
            for (var entry : map.entrySet()) {
                String child = path + "/" + entry.getKey();
                if ("description".equals(entry.getKey()) && entry.getValue() instanceof String text) descriptions.put(child, text);
                else collect(entry.getValue(), child, descriptions);
            }
        } else if (node instanceof List<?> list) {
            for (int i = 0; i < list.size(); i++) collect(list.get(i), path + "/" + i, descriptions);
        }
    }
}
