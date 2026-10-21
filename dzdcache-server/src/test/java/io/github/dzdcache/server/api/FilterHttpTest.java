package io.github.dzdcache.server.api;

import io.github.dzdcache.server.DzdCacheApplication;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;

class FilterHttpTest {
    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static final String JSON_ONLY = """
        {"id":"people","version":1,"identity":"/id","capacity":100,"ttl":"infinite",
         "dimensions":[{"name":"city","path":"/city","index":"exact","type":"string"}]}
        """;
    private static final String WITH_SCHEMA = """
        {"id":"people","version":1,"identity":"/id","capacity":100,"ttl":"infinite",
         "dimensions":[{"name":"city","path":"/city","index":"exact"}],
         "schema":{"type":"object","required":["id","city"],"properties":{
           "id":{"type":"integer"},"city":{"type":"string"},"age":{"type":"integer"},"name":{"type":"string"},
           "tags":{"type":"array","items":{"type":"string"}}}}}
        """;

    @ParameterizedTest @ValueSource(strings = {"json", "schema"})
    void searchesNarrowByFiltersCountAndScanInTheCache(String kind) throws Exception {
        try (ConfigurableApplicationContext app = new SpringApplicationBuilder(DzdCacheApplication.class).run("--server.port=0",
                "--dzdcache.api-token=client", "--dzdcache.max-scan=12", "--logging.level.root=ERROR", "--spring.main.banner-mode=off");
             HttpClient http = HttpClient.newHttpClient()) {
            int port = app.getEnvironment().getRequiredProperty("local.server.port", Integer.class);
            assertThat(post(http, port, "/cache/schemas", kind.equals("json") ? JSON_ONLY : WITH_SCHEMA).statusCode()).isEqualTo(201);
            for (int id = 1; id <= 10; id++) {
                String person = "{\"id\":" + id + ",\"city\":\"" + (id % 2 == 0 ? "Oslo" : "Bergen") + "\",\"age\":" + (20 + id)
                    + ",\"name\":\"N" + id + "\",\"tags\":" + (id % 3 == 0 ? "[\"chess\",\"piano\"]" : "[\"football\"]") + "}";
                assertThat(post(http, port, "/cache/entries", "{\"specification\":\"people\",\"schemaVersion\":1,\"revision\":1,\"value\":" + person + "}")
                    .statusCode()).isEqualTo(200);
            }
            String oslo = "{\"specification\":\"people\",\"schemaVersion\":1,\"dimension\":\"city\",\"value\":\"Oslo\",";
            JsonNode older = JSON.readTree(post(http, port, "/cache/search", oslo
                + "\"filter\":{\"path\":\"/age\",\"op\":\"gte\",\"value\":26},\"fields\":[\"/id\"]}").body());
            assertThat(older.valueStream().map(node -> node.path("id").asInt()).toList()).containsExactlyInAnyOrder(6, 8, 10);
            JsonNode chess = JSON.readTree(post(http, port, "/cache/search", oslo
                + "\"filter\":{\"all\":[{\"path\":\"/tags/*\",\"op\":\"eq\",\"value\":\"chess\"},{\"not\":{\"path\":\"/name\",\"op\":\"prefix\",\"value\":\"N1\"}}]},\"fields\":[\"/id\"]}").body());
            assertThat(chess.valueStream().map(node -> node.path("id").asInt()).toList()).containsExactly(6);
            assertThat(JSON.readTree(post(http, port, "/cache/search", oslo + "\"count\":true}").body()).path("count").asInt()).isEqualTo(5);
            JsonNode byTag = JSON.readTree(post(http, port, "/cache/search",
                "{\"specification\":\"people\",\"schemaVersion\":1,\"scan\":true,\"countBy\":\"/tags/*\"}").body());
            assertThat(byTag.path("football").asInt()).isEqualTo(7);
            assertThat(byTag.path("chess").asInt()).isEqualTo(3);
            JsonNode scanned = JSON.readTree(post(http, port, "/cache/search", "{\"specification\":\"people\",\"schemaVersion\":1,\"scan\":true,"
                + "\"filter\":{\"path\":\"/age\",\"op\":\"in\",\"value\":[21,30]},\"fields\":[\"/id\"]}").body());
            assertThat(scanned.valueStream().map(node -> node.path("id").asInt()).toList()).containsExactlyInAnyOrder(1, 10);
            for (int id = 11; id <= 13; id++)
                post(http, port, "/cache/entries", "{\"specification\":\"people\",\"schemaVersion\":1,\"revision\":1,\"value\":{\"id\":" + id + ",\"city\":\"Oslo\"}}");
            assertThat(post(http, port, "/cache/search", "{\"specification\":\"people\",\"schemaVersion\":1,\"scan\":true,\"count\":true}").statusCode())
                .as("a scan over more than max-scan roots").isEqualTo(422);
            assertThat(post(http, port, "/cache/search", oslo + "\"filter\":{\"path\":\"/age\",\"op\":\"between\",\"value\":1}}").statusCode()).isEqualTo(400);
            assertThat(post(http, port, "/cache/search", "{\"specification\":\"people\",\"schemaVersion\":1,\"dimension\":\"city\",\"value\":\"Oslo\",\"scan\":true}")
                .statusCode()).isEqualTo(400);
            if (kind.equals("schema"))
                assertThat(post(http, port, "/cache/search", oslo + "\"filter\":{\"path\":\"/height\",\"op\":\"gt\",\"value\":1}}").statusCode())
                    .as("a path the schema does not declare").isEqualTo(400);
        }
    }

    private static HttpResponse<String> post(HttpClient http, int port, String path, String body) throws Exception {
        return http.send(HttpRequest.newBuilder(URI.create("http://localhost:" + port + path)).timeout(Duration.ofSeconds(10))
            .header("Authorization", "Bearer client").header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(body)).build(), HttpResponse.BodyHandlers.ofString());
    }
}
