package io.github.dzdcache.server.api;

import io.github.dzdcache.server.DzdCacheApplication;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;

class TextSearchHttpTest {
    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static final String DIMENSIONS = """
        "dimensions":[{"name":"name","path":"/name","index":"prefix","type":"string","strategies":[{"split":"whitespace"},"contains"]},
                      {"name":"city","path":"/city","index":"exact","type":"string"}]""";
    private static final String JSON_ONLY = "{\"id\":\"students\",\"version\":1,\"identity\":\"/id\",\"capacity\":100,\"ttl\":\"infinite\"," + DIMENSIONS + "}";
    private static final String WITH_SCHEMA = "{\"id\":\"students\",\"version\":1,\"identity\":\"/id\",\"capacity\":100,\"ttl\":\"infinite\"," + DIMENSIONS.replace(",\"type\":\"string\"", "")
        + ",\"schema\":{\"type\":\"object\",\"required\":[\"id\",\"name\",\"city\"],\"properties\":{"
        + "\"id\":{\"type\":\"integer\"},\"name\":{\"type\":\"string\"},\"city\":{\"type\":\"string\"}}}}";
    private static final String SEARCH = "{\"specification\":\"students\",\"schemaVersion\":1,\"dimension\":\"name\",";

    @ParameterizedTest @ValueSource(strings = {"json", "schema"})
    void wordsFuzzyAndContainsSearchesComeBestFirstAndCanSayHowWellEachMatched(String kind) throws Exception {
        try (ConfigurableApplicationContext app = new SpringApplicationBuilder(DzdCacheApplication.class).run("--server.port=0",
                "--dzdcache.api-token=client", "--logging.level.root=ERROR", "--spring.main.banner-mode=off");
             HttpClient http = HttpClient.newHttpClient()) {
            int port = app.getEnvironment().getRequiredProperty("local.server.port", Integer.class);
            HttpResponse<String> published = post(http, port, "/cache/schemas", kind.equals("json") ? JSON_ONLY : WITH_SCHEMA);
            assertThat(published.statusCode()).as(published.body()).isEqualTo(201);
            String[] names = {"Olsen Anna", "Anna Maria Olsen", "Anna Olsen", "Per Andersen"};
            for (int id = 1; id <= names.length; id++)
                assertThat(post(http, port, "/cache/entries", "{\"specification\":\"students\",\"schemaVersion\":1,\"revision\":1,\"value\":"
                    + "{\"id\":" + id + ",\"name\":\"" + names[id - 1] + "\",\"city\":\"Oslo\"}}").statusCode()).isEqualTo(200);

            assertThat(ids(post(http, port, "/cache/search", SEARCH + "\"value\":\"anna olsen\",\"match\":\"words\",\"fields\":[\"/id\"]}")))
                .containsExactly(3, 2, 1);
            JsonNode ranked = JSON.readTree(post(http, port, "/cache/search", SEARCH + "\"value\":\"anna olsen\",\"match\":\"words\",\"ranked\":true}").body());
            assertThat(ranked.valueStream().map(result -> result.path("rankLevel").asString()).toList())
                .containsExactly("EXACT", "WORDS_IN_ORDER", "WORDS_IN_ANY_ORDER");
            assertThat(ranked.get(0).path("value").path("name").asString()).isEqualTo("Anna Olsen");
            assertThat(ids(post(http, port, "/cache/search", SEARCH + "\"value\":\"ana olsne\",\"match\":\"fuzzy\",\"maxEdits\":1,\"fields\":[\"/id\"]}")))
                .containsExactlyInAnyOrder(1, 2, 3);
            assertThat(ids(post(http, port, "/cache/search", SEARCH + "\"value\":\"ders\",\"match\":\"contains\",\"fields\":[\"/id\"]}")))
                .containsExactly(4);
            assertThat(ids(post(http, port, "/cache/search", SEARCH + "\"value\":\"anna o\",\"fields\":[\"/id\"]}"))).containsExactly(3);
            assertThat(JSON.readTree(post(http, port, "/cache/search", SEARCH + "\"value\":\"olsen\",\"match\":\"words\",\"count\":true}").body())
                .path("count").asInt()).isEqualTo(3);

            assertThat(post(http, port, "/cache/search", SEARCH + "\"value\":\"anna\",\"match\":\"words\",\"maxEdits\":1}").statusCode()).isEqualTo(400);
            assertThat(post(http, port, "/cache/search", SEARCH + "\"value\":\"anna\",\"match\":\"sounds-like\"}").statusCode()).isEqualTo(400);
            assertThat(post(http, port, "/cache/search", SEARCH + "\"value\":\"anna\",\"match\":\"words\",\"ranked\":true,\"count\":true}").statusCode())
                .isEqualTo(400);
            assertThat(post(http, port, "/cache/search", "{\"specification\":\"students\",\"schemaVersion\":1,\"dimension\":\"city\",\"value\":\"osl\","
                + "\"match\":\"words\"}").statusCode()).as("a strategy the dimension does not declare").isEqualTo(400);
            assertThat(post(http, port, "/cache/search", "{\"specification\":\"students\",\"schemaVersion\":1,\"scan\":true,\"match\":\"words\"}")
                .statusCode()).isEqualTo(400);
        }
    }

    @ParameterizedTest @ValueSource(strings = {
        "{\"name\":\"city\",\"path\":\"/city\",\"index\":\"exact\",\"type\":\"string\",\"strategies\":[\"contains\"]}",
        "{\"name\":\"name\",\"path\":\"/name\",\"index\":\"prefix\",\"type\":\"string\",\"strategies\":[\"soundex\"]}",
        "{\"name\":\"name\",\"path\":\"/name\",\"index\":\"prefix\",\"type\":\"string\",\"strategies\":[{\"split\":\"commas\"}]}",
        "{\"name\":\"tags\",\"path\":\"/tags/*\",\"index\":\"prefix\",\"type\":\"string\",\"strategies\":[\"contains\"]}"
    })
    void strategiesAreOnlyForASingleValuedPrefixDimensionAndMustBeKnown(String dimension) throws Exception {
        try (ConfigurableApplicationContext app = new SpringApplicationBuilder(DzdCacheApplication.class).run("--server.port=0",
                "--dzdcache.api-token=client", "--logging.level.root=ERROR", "--spring.main.banner-mode=off");
             HttpClient http = HttpClient.newHttpClient()) {
            int port = app.getEnvironment().getRequiredProperty("local.server.port", Integer.class);
            assertThat(post(http, port, "/cache/schemas", "{\"id\":\"students\",\"version\":1,\"identity\":\"/id\",\"capacity\":100,\"ttl\":\"infinite\","
                + "\"dimensions\":[" + dimension + "]}").statusCode()).isEqualTo(400);
        }
    }

    private static List<Integer> ids(HttpResponse<String> response) {
        assertThat(response.statusCode()).as(response.body()).isEqualTo(200);
        return JSON.readTree(response.body()).valueStream().map(node -> node.path("id").asInt()).toList();
    }

    private static HttpResponse<String> post(HttpClient http, int port, String path, String body) throws Exception {
        return http.send(HttpRequest.newBuilder(URI.create("http://localhost:" + port + path)).timeout(Duration.ofSeconds(10))
            .header("Authorization", "Bearer client").header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(body)).build(), HttpResponse.BodyHandlers.ofString());
    }
}
