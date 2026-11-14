package io.github.dzdcache.server.api;

import io.github.dzdcache.server.DzdCacheApplication;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;

class SpecificationChangeHttpTest {
    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static final String WITHOUT = "{\"id\":\"students\",\"version\":1,\"identity\":\"/id\",\"capacity\":100,\"ttl\":\"infinite\"}";
    private static final String WITH = "{\"id\":\"students\",\"version\":1,\"identity\":\"/id\",\"capacity\":100,\"ttl\":\"infinite\",\"dimensions\":["
        + "{\"name\":\"name\",\"path\":\"/name\",\"index\":\"prefix\",\"type\":\"string\",\"strategies\":[{\"split\":\"whitespace\"}]},"
        + "{\"name\":\"age\",\"path\":\"/age\",\"index\":\"range\",\"type\":\"integer\"}]}";

    @Test void aSearchSpecificationIsRegisteredAfterwardsAndItsStatusCanBeFollowed() throws Exception {
        try (ConfigurableApplicationContext app = new SpringApplicationBuilder(DzdCacheApplication.class).run("--server.port=0",
                "--dzdcache.api-token=client", "--logging.level.root=ERROR", "--spring.main.banner-mode=off");
             HttpClient http = HttpClient.newHttpClient()) {
            int port = app.getEnvironment().getRequiredProperty("local.server.port", Integer.class);
            assertThat(send(http, port, "POST", "/cache/schemas", WITHOUT).statusCode()).isEqualTo(201);
            for (int id = 1; id <= 3; id++)
                assertThat(send(http, port, "POST", "/cache/entries", "{\"specification\":\"students\",\"schemaVersion\":1,\"revision\":1,\"value\":"
                    + "{\"id\":" + id + ",\"name\":\"" + (id == 3 ? "Per Berg" : "Anna" + id + " Olsen") + "\",\"age\":" + (20 + id) + "}}").statusCode()).isEqualTo(200);
            assertThat(status(http, port).path("status").asString()).isEqualTo("NONE_REGISTERED");

            HttpResponse<String> accepted = send(http, port, "PUT", "/cache/schemas/students/1", WITH);
            assertThat(accepted.statusCode()).as(accepted.body()).isEqualTo(202);
            assertThat(JSON.readTree(accepted.body()).path("status").asString()).isIn("UPDATING", "COMPLETED");
            long deadline = System.nanoTime() + 10_000_000_000L;
            while (!status(http, port).path("status").asString().equals("COMPLETED")) {
                assertThat(System.nanoTime()).as("the change is switched on").isLessThan(deadline);
                Thread.sleep(20);
            }
            HttpResponse<String> olsen = send(http, port, "POST", "/cache/search",
                "{\"specification\":\"students\",\"schemaVersion\":1,\"dimension\":\"name\",\"value\":\"olsen\",\"match\":\"words\",\"fields\":[\"/id\"]}");
            assertThat(JSON.readTree(olsen.body()).valueStream().map(node -> node.path("id").asInt()).sorted().toList()).containsExactly(1, 2);
            HttpResponse<String> young = send(http, port, "POST", "/cache/search",
                "{\"specification\":\"students\",\"schemaVersion\":1,\"dimension\":\"age\",\"from\":21,\"to\":22,\"fields\":[\"/id\"]}");
            assertThat(JSON.readTree(young.body()).valueStream().map(node -> node.path("id").asInt()).sorted().toList()).containsExactly(1, 2);
            assertThat(JSON.readTree(send(http, port, "GET", "/cache/schemas/students/1", null).body()).path("dimensions")).hasSize(2);
            assertThat(send(http, port, "POST", "/cache/schemas", WITHOUT).statusCode()).as("the original definition published again").isEqualTo(201);

            assertThat(send(http, port, "PUT", "/cache/schemas/teachers/1", WITH).statusCode()).isEqualTo(400);
            assertThat(send(http, port, "PUT", "/cache/schemas/students/1", WITH.replace("\"capacity\":100", "\"capacity\":200")).statusCode()).isEqualTo(400);
            assertThat(send(http, port, "PUT", "/cache/schemas/students/1", WITH.replace(",\"strategies\":[{\"split\":\"whitespace\"}]", "")).statusCode())
                .as("a strategy taken off").isEqualTo(400);
            assertThat(send(http, port, "GET", "/cache/schemas/teachers/1/status", null).statusCode()).isEqualTo(404);
        }
    }

    private static JsonNode status(HttpClient http, int port) throws Exception {
        HttpResponse<String> response = send(http, port, "GET", "/cache/schemas/students/1/status", null);
        assertThat(response.statusCode()).as(response.body()).isEqualTo(200);
        return JSON.readTree(response.body());
    }

    private static HttpResponse<String> send(HttpClient http, int port, String method, String path, String body) throws Exception {
        return http.send(HttpRequest.newBuilder(URI.create("http://localhost:" + port + path)).timeout(Duration.ofSeconds(10))
            .header("Authorization", "Bearer client").header("Content-Type", "application/json")
            .method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body)).build(), HttpResponse.BodyHandlers.ofString());
    }
}
