package io.github.dzdcache.server.api;

import io.github.dzdcache.server.DzdCacheApplication;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;

class ChangeStreamHttpTest {
    private static final String SCHEMA = """
        {"id":"people","version":1,"identity":"/id","capacity":100,"ttl":"infinite",
         "dimensions":[{"name":"city","path":"/city","index":"exact","type":"string"}]}
        """;

    @Test void changesArriveAsServerSentEventsAndResumeAfterTheLastEventId() throws Exception {
        try (ConfigurableApplicationContext app = new SpringApplicationBuilder(DzdCacheApplication.class).run("--server.port=0",
                "--dzdcache.api-token=client", "--logging.level.root=ERROR", "--spring.main.banner-mode=off");
             HttpClient http = HttpClient.newHttpClient()) {
            int port = app.getEnvironment().getRequiredProperty("local.server.port", Integer.class);
            assertThat(post(http, port, "/cache/schemas", SCHEMA).statusCode()).isEqualTo(201);
            List<String> first = new CopyOnWriteArrayList<>();
            Thread reader = listen(http, port, null, first);
            Thread.sleep(300);
            for (int id = 1; id <= 3; id++)
                post(http, port, "/cache/entries", "{\"specification\":\"people\",\"schemaVersion\":1,\"revision\":1,\"value\":{\"id\":" + id + ",\"city\":\"Oslo\"}}");
            post(http, port, "/cache/entries/invalidate", "{\"specification\":\"people\",\"schemaVersion\":1,\"identity\":2,\"revision\":2}");
            await(() -> first.stream().filter(line -> line.startsWith("data:")).count() >= 4);
            List<String> data = first.stream().filter(line -> line.startsWith("data:")).toList();
            assertThat(data.get(0)).contains("\"kind\":\"PUT\"", "\"identity\":1", "\"city\":\"Oslo\"");
            assertThat(data.get(3)).contains("\"kind\":\"INVALIDATED\"", "\"identity\":2");
            assertThat(first).anyMatch(line -> line.equals("event:PUT")).anyMatch(line -> line.equals("event:INVALIDATED"));
            String firstId = first.stream().filter(line -> line.startsWith("id:")).findFirst().orElseThrow().substring(3);
            reader.interrupt();

            List<String> resumed = new CopyOnWriteArrayList<>();
            Thread again = listen(http, port, firstId, resumed);
            await(() -> resumed.stream().filter(line -> line.startsWith("data:")).count() >= 3);
            List<String> resumedData = resumed.stream().filter(line -> line.startsWith("data:")).toList();
            assertThat(resumedData.getFirst()).contains("\"identity\":2");
            again.interrupt();
        }
    }

    private static Thread listen(HttpClient http, int port, String lastEventId, List<String> lines) {
        return Thread.ofVirtual().start(() -> {
            try {
                HttpRequest.Builder request = HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/cache/changes?specification=people&schemaVersion=1"))
                    .header("Authorization", "Bearer client");
                if (lastEventId != null) request.header("Last-Event-ID", lastEventId);
                HttpResponse<Stream<String>> response = http.send(request.build(), HttpResponse.BodyHandlers.ofLines());
                response.body().forEach(lines::add);
            } catch (Exception ignored) { }
        });
    }

    private static void await(java.util.function.BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) Thread.sleep(25);
        assertThat(condition.getAsBoolean()).isTrue();
    }

    private static HttpResponse<String> post(HttpClient http, int port, String path, String body) throws Exception {
        return http.send(HttpRequest.newBuilder(URI.create("http://localhost:" + port + path)).timeout(Duration.ofSeconds(10))
            .header("Authorization", "Bearer client").header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(body)).build(), HttpResponse.BodyHandlers.ofString());
    }
}
