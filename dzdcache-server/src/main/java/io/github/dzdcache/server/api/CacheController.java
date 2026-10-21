package io.github.dzdcache.server.api;

import io.github.dzdcache.cache.*;
import io.github.dzdcache.cluster.ClusterCoordinator;
import io.github.dzdcache.server.schema.SchemaDefinition;
import io.github.dzdcache.server.schema.ServerTypeFactory;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.util.List;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import tools.jackson.databind.JsonNode;

@RestController
@RequestMapping("/cache")
public class CacheController {
    private final EngineHolder engines;
    private final ClusterCoordinator cluster;
    private final MeterRegistry meters;
    private final ServerTypeFactory schemas;
    private final tools.jackson.databind.ObjectMapper mapper;
    public CacheController(EngineHolder engines, ClusterCoordinator cluster, MeterRegistry meters, ServerTypeFactory schemas,
                           io.github.dzdcache.spring.DzdCacheMapper mapper) {
        this.engines = engines; this.cluster = cluster; this.meters = meters; this.schemas = schemas; this.mapper = mapper.mapper();
    }
    private void mutated(EntryMutation.Operation operation, int status) {
        meters.counter("dzdcache.mutations", "operation", operation.name().toLowerCase(java.util.Locale.ROOT), "status", Integer.toString(status)).increment();
    }
    private EntryMutation mutate(EntryMutation.Operation operation, java.util.function.Supplier<ClusterCoordinator.Outcome> write) {
        ClusterCoordinator.Outcome outcome;
        try { outcome = write.get(); }
        catch (CacheException e) { mutated(operation, e.status()); throw e; }
        mutated(operation, outcome.status());
        if (outcome.status() != 200) throw new CacheException(outcome.status(), outcome.error());
        return outcome.mutation();
    }

    @PostMapping("/schemas") @ResponseStatus(HttpStatus.CREATED)
    public SchemaView publish(@RequestBody SchemaDefinition schema) {
        if (schema == null) throw new IllegalArgumentException("A schema definition is required");
        return cluster.publish(schemas.describe(schema));
    }
    @GetMapping("/schemas") public List<SchemaDefinition> schemas() {
        cluster.awaitCurrent();
        return engines.engine().descriptors().stream().filter(ServerTypeFactory::describes)
            .map(type -> mapper.treeToValue(type.definition(), SchemaDefinition.class)).toList();
    }
    @GetMapping("/schemas/{id}/{version}") public SchemaDefinition schema(@PathVariable String id, @PathVariable int version) {
        cluster.awaitCurrent();
        TypeDescriptor type = engines.engine().descriptor(id, version);
        if (!ServerTypeFactory.describes(type)) throw new CacheException(404, "Not a JSON specification");
        return mapper.treeToValue(type.definition(), SchemaDefinition.class);
    }
    public record ResizeRequest(Integer size, List<String> leaving) {}
    @GetMapping("/cluster") public ClusterCoordinator.ClusterView clusterView() {
        cluster.requireReady();
        return cluster.view();
    }
    @PutMapping("/cluster") public ClusterCoordinator.ClusterView resize(@RequestBody ResizeRequest request) {
        if (request == null || request.size() == null) throw new IllegalArgumentException("A size is required");
        cluster.resize(request.size(), java.util.Set.copyOf(request.leaving() == null ? List.of() : request.leaving()));
        return cluster.view();
    }
    public record IdentityRequest(String specification, int schemaVersion, JsonNode identity, List<String> fields) {}
    public record RemoveRequest(String specification, int schemaVersion, JsonNode identity, Long revision) {}
    public record SearchRequest(String specification, int schemaVersion, String dimension, JsonNode value, JsonNode from, JsonNode to,
                                Integer limit, List<String> fields) {}

    @PostMapping("/entries") public EntryMutation put(@RequestBody PutRequest request) {
        return mutate(EntryMutation.Operation.PUT, () -> cluster.put(java.util.Collections.singletonList(request)).getFirst());
    }
    /** A bulk operation reports individual outcomes; earlier successful entries are not rolled back. */
    public record BatchResult(int index, int status, EntryMutation mutation, String error) {}
    @PostMapping("/entries/batch") public List<BatchResult> batch(@RequestBody List<PutRequest> requests) {
        if (requests.size() > 1000) throw new IllegalArgumentException("Batch limit is 1000 entries");
        List<ClusterCoordinator.Outcome> outcomes;
        try { outcomes = requests.isEmpty() ? List.of() : cluster.put(requests); }
        catch (CacheException e) { outcomes = java.util.Collections.nCopies(requests.size(), new ClusterCoordinator.Outcome(e.status(), e.getMessage(), null)); }
        List<BatchResult> result = new java.util.ArrayList<>(outcomes.size());
        for (int i = 0; i < outcomes.size(); i++) {
            var outcome = outcomes.get(i);
            result.add(new BatchResult(i, outcome.status(), outcome.status() == 200 ? outcome.mutation() : null, outcome.error()));
            mutated(EntryMutation.Operation.PUT, outcome.status());
        }
        return result;
    }
    private static ResponseEntity.BodyBuilder answer(ResponseEntity.BodyBuilder response, ClusterCoordinator.Read read) {
        return read.fresh() ? response : response.header("X-Dzdcache-Stale", "true").header("X-Dzdcache-Staleness-Ms", Long.toString(read.staleMillis()));
    }
    @PostMapping("/entries/get") public ResponseEntity<byte[]> get(@RequestBody IdentityRequest request) {
        ClusterCoordinator.Read read = cluster.awaitCurrent();
        Timer.Sample sample = Timer.start(meters);
        byte[] graph = engines.engine().getJson(request.specification(), request.schemaVersion(), request.identity(), request.fields());
        sample.stop(meters.timer("dzdcache.get", "specification", request.specification(), "outcome", graph == null ? "miss" : "hit"));
        return graph == null ? answer(ResponseEntity.status(404), read).build() : answer(ResponseEntity.ok(), read).contentType(MediaType.APPLICATION_JSON).body(graph);
    }
    @PostMapping("/search") public ResponseEntity<byte[]> search(@RequestBody SearchRequest request) {
        ClusterCoordinator.Read read = cluster.awaitCurrent();
        Timer.Sample sample = Timer.start(meters);
        CacheEngine.Rendered found = engines.engine().searchJson(request.specification(), request.schemaVersion(), request.dimension(),
            request.value(), request.from(), request.to(), request.limit(), request.fields());
        sample.stop(meters.timer("dzdcache.search", "specification", request.specification(), "dimension", request.dimension()));
        meters.summary("dzdcache.search.results", "specification", request.specification()).record(found.count());
        return answer(ResponseEntity.ok(), read).contentType(MediaType.APPLICATION_JSON).body(found.json());
    }
    @PostMapping("/entries/invalidate") @ResponseStatus(HttpStatus.NO_CONTENT)
    public void invalidate(@RequestBody RemoveRequest request) {
        EntryMutation mutation = new EntryMutation(EntryMutation.Operation.INVALIDATE, request.specification(), request.schemaVersion(),
            request.identity(), null, request.revision() == null ? 0 : request.revision(), null);
        mutate(mutation.operation(), () -> cluster.change(mutation));
    }
    @PostMapping("/entries/evict") @ResponseStatus(HttpStatus.NO_CONTENT)
    public void evict(@RequestBody RemoveRequest request) {
        EntryMutation mutation = new EntryMutation(EntryMutation.Operation.EVICT, request.specification(), request.schemaVersion(),
            request.identity(), null, request.revision() == null ? 0 : request.revision(), null);
        mutate(mutation.operation(), () -> cluster.change(mutation));
    }
}
