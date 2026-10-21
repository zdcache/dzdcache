package io.github.dzdcache.server.api;

import io.github.dzdcache.cache.*;
import io.github.dzdcache.cluster.ClusterCoordinator;
import io.github.dzdcache.json.SchemaDefinition;
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
                                Integer limit, List<String> fields, JsonNode filter, Boolean count, String countBy, Boolean scan) {}

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
        CacheEngine engine = engines.engine();
        boolean scan = Boolean.TRUE.equals(request.scan());
        CacheEngine.Selection selection = engine.select(request.specification(), request.schemaVersion(), request.dimension(), request.value(),
            request.from(), request.to(), request.filter(), scan);
        String dimension = scan ? "(scan)" : request.dimension();
        String filtered = Boolean.toString(selection.filter() != null);
        byte[] body;
        int count;
        if (Boolean.TRUE.equals(request.count()) || request.countBy() != null) {
            if (request.fields() != null || request.limit() != null) throw new IllegalArgumentException("'count' and 'countBy' take no 'fields' or 'limit'");
            if (request.countBy() != null) {
                java.util.Map<Object, Long> counts = engine.countByJson(request.specification(), request.schemaVersion(), selection, request.countBy());
                java.util.Map<String, Long> named = new java.util.LinkedHashMap<>();
                counts.forEach((value, n) -> named.put(String.valueOf(value instanceof java.math.BigDecimal number ? number.toPlainString() : value), n));
                body = mapper.writeValueAsBytes(named);
                count = named.size();
            } else {
                long total = engine.countJson(request.specification(), request.schemaVersion(), selection);
                body = mapper.writeValueAsBytes(java.util.Map.of("count", total));
                count = 1;
            }
        } else {
            CacheEngine.Rendered found = engine.searchJson(request.specification(), request.schemaVersion(), selection, request.limit(), request.fields());
            body = found.json();
            count = found.count();
        }
        sample.stop(meters.timer("dzdcache.search", "specification", request.specification(), "dimension", dimension, "filtered", filtered));
        meters.summary("dzdcache.search.results", "specification", request.specification()).record(count);
        return answer(ResponseEntity.ok(), read).contentType(MediaType.APPLICATION_JSON).body(body);
    }
    @GetMapping(value = "/changes", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public org.springframework.web.servlet.mvc.method.annotation.SseEmitter changes(@RequestParam(required = false) String specification,
            @RequestParam(required = false) Integer schemaVersion, @RequestParam(required = false) String from,
            @RequestHeader(value = "Last-Event-ID", required = false) String lastEventId) {
        cluster.requireReady();
        if (specification != null && schemaVersion != null && !engines.engine().contains(specification, schemaVersion))
            throw new CacheException(404, "Unknown specification or version");
        String resume = lastEventId != null ? lastEventId : from;
        var emitter = new org.springframework.web.servlet.mvc.method.annotation.SseEmitter(0L);
        io.github.dzdcache.cluster.ChangeHub.Subscription[] subscription = new io.github.dzdcache.cluster.ChangeHub.Subscription[1];
        subscription[0] = cluster.changes().subscribe(resume == null ? null : io.github.dzdcache.cluster.ChangeHub.Position.parse(resume),
            change -> (specification == null || specification.equals(change.specification()))
                && (schemaVersion == null || schemaVersion == change.schemaVersion()),
            event -> {
                var change = event.change();
                var data = new java.util.LinkedHashMap<String, Object>();
                data.put("position", event.position().toString());
                data.put("kind", change.kind().name());
                if (change.specification() != null) {
                    data.put("specification", change.specification());
                    data.put("schemaVersion", change.schemaVersion());
                    data.put("identity", change.identity());
                    data.put("revision", change.revision());
                }
                if (change.value() != null) data.put("value", change.value());
                data.put("at", change.at());
                try {
                    emitter.send(org.springframework.web.servlet.mvc.method.annotation.SseEmitter.event().id(event.position().toString())
                        .name(change.kind().name()).data(mapper.writeValueAsString(data), MediaType.APPLICATION_JSON));
                } catch (java.io.IOException | IllegalStateException e) {
                    if (subscription[0] != null) subscription[0].close();
                }
            });
        emitter.onCompletion(() -> subscription[0].close());
        emitter.onTimeout(() -> subscription[0].close());
        emitter.onError(error -> subscription[0].close());
        return emitter;
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
