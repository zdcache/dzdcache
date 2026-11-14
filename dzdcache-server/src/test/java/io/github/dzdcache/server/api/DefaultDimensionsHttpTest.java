package io.github.dzdcache.server.api;

import io.github.dzdcache.server.DzdCacheApplication;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;

class DefaultDimensionsHttpTest {
    private static final JsonMapper JSON = JsonMapper.builder().build();

    @Test void aTypeWithDefaultDimensionsIsSearchableOnWhatItHoldsAndShowsWhatWasFound() throws Exception {
        try (ConfigurableApplicationContext app = new SpringApplicationBuilder(DzdCacheApplication.class).run("--server.port=0",
                "--dzdcache.api-token=client", "--logging.level.root=ERROR", "--spring.main.banner-mode=off");
             HttpClient http = HttpClient.newHttpClient()) {
            int port = app.getEnvironment().getRequiredProperty("local.server.port", Integer.class);
            assertThat(send(http, port, "POST", "/cache/schemas",
                "{\"id\":\"students\",\"version\":1,\"identity\":\"/id\",\"capacity\":100,\"ttl\":\"infinite\",\"dimensions\":\"default\"}").statusCode()).isEqualTo(201);
            assertThat(send(http, port, "POST", "/cache/entries", "{\"specification\":\"students\",\"schemaVersion\":1,\"revision\":1,\"value\":"
                + "{\"id\":1,\"name\":\"Anna Olsen\",\"nickname\":null,\"address\":{\"city\":\"Oslo\"}}}").statusCode()).isEqualTo(200);
            JsonNode stored = JSON.readTree(send(http, port, "POST", "/cache/entries/get", "{\"specification\":\"students\",\"schemaVersion\":1,\"identity\":1}").body());
            assertThat(stored.has("nickname")).as("a null field is kept").isTrue();
            assertThat(stored.path("nickname").isNull()).isTrue();
            JsonNode definition = JSON.readTree(send(http, port, "GET", "/cache/schemas/students/1", null).body());
            assertThat(definition.path("dimensions").asString()).isEqualTo("default");
            assertThat(definition.path("discovered").valueStream().map(dimension -> dimension.path("name").asString()).toList())
                .containsExactly("id", "name", "address.city");
            HttpResponse<String> oslo = send(http, port, "POST", "/cache/search",
                "{\"specification\":\"students\",\"schemaVersion\":1,\"dimension\":\"address.city\",\"value\":\"Oslo\",\"fields\":[\"/id\"]}");
            assertThat(JSON.readTree(oslo.body()).valueStream().map(node -> node.path("id").asInt()).toList()).containsExactly(1);
            assertThat(JSON.readTree(send(http, port, "GET", "/cache/schemas/students/1/status", null).body()).path("status").asString()).isEqualTo("COMPLETED");
            assertThat(send(http, port, "POST", "/cache/schemas", "{\"id\":\"teachers\",\"version\":1,\"identity\":\"/id\",\"capacity\":100,"
                + "\"dimensions\":\"default\",\"schema\":{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"integer\"}}}}").statusCode())
                .as("default is for JSON types without a schema").isEqualTo(400);
        }
    }

    private static HttpResponse<String> send(HttpClient http, int port, String method, String path, String body) throws Exception {
        return http.send(HttpRequest.newBuilder(URI.create("http://localhost:" + port + path)).timeout(Duration.ofSeconds(10))
            .header("Authorization", "Bearer client").header("Content-Type", "application/json")
            .method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body)).build(), HttpResponse.BodyHandlers.ofString());
    }
}
