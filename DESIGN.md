# Distributed ZDCache (DZDCache) design discussion

> Historical design discussion. See [README](README.md) for the implemented service and [consistency limits](docs/CONSISTENCY.md) for the current replication contract. The strict freshness goal below is not fully guaranteed by the initial implementation.

> Latest scope decision: publishers explicitly register schemas and publish/update/invalidate objects; consumers own data-source selection and fallback. The generation workflow below is optional external tooling, not a DZDCache requirement or committed service deliverable. See the current [responsibility split](README.md#responsibilities).

This document records the original architecture discussion for Distributed ZDCache (DZDCache). It is a design proposal, not an implementation specification. The choices under **Agreed direction** reflect that discussion; **Open decisions** identifies behavior that was still to be settled. The later [single-node service proposal](SINGLE_NODE_SERVICE.md) develops the design around the implemented ZDCache library.

## Goal

Provide a cache that can be used locally inside an application or as a distributed, Kubernetes-hosted service. Generate cache integrations around Java clients produced by OpenAPI Generator using its `webclient` library. A human decides which API operations are cached.

## Agreed direction

### Two ways to use the cache

- **Local JAR:** A plain Java cache library that a Spring Boot service can embed. Its state belongs to that process and is not distributed. Keeping the core independent of Spring would also allow ordinary Java applications to use it.
- **Distributed service:** A separate Spring Boot application deployed as multiple cache nodes in Kubernetes. Other services access it through generated integration code. Cache failure causes the consumer to call the original service.

### Generation workflow

1. OpenAPI Generator generates a Java `webclient` for an API.
2. Our inspection tool examines that generated code and writes a YAML or JSON manifest of operations that *might* be cacheable.
3. A human reviews the manifest, enables selected operations, and sets their policies. The tool must preserve those choices when the OpenAPI client is regenerated.
4. Our generator uses the reviewed manifest to produce two kinds of integration:
   - **Consumer mode:** subclasses or otherwise wraps the relevant generated API clients. A selected read checks the distributed cache, calls the original API on a miss or cache failure, and returns the result.
   - **Owner mode:** integrates with the service that owns the data. It invalidates or replaces affected cache entries when that service changes data. This integration belongs at the owner's data-change boundary, rather than in a client that calls its own API.

The two modes share the definitions of cache keys and the relationship between read operations and data changes. Exact subclass signatures and constructor handling depend on the generated `webclient` version and options; these need verification against representative output before code generation is specified.

### Manifest policy

Each selected operation should be able to define:

- Whether caching is enabled.
- How to construct a key from the operation and relevant arguments, including caller identity when the response is caller-specific.
- Which data changes invalidate or replace that key.
- A time to live, including **infinite** (no expiry). Infinite TTL still permits explicit invalidation and does not imply durability across pod restarts or immunity from future memory eviction.
- Whether a consumer may **backfill** the cache after an original-service call.

Cacheability is not inferred solely from an HTTP `GET`: a human must decide whether the result is safe to share and how long it remains valid. Errors should not be cached by default. A cache miss must be distinct from a cached empty or null-like result.

### Consumer read behavior

```text
Generated client call
        |
        v
Distributed cache lookup
   | hit           | miss or cache failure
   v               v
Return value    Call original API
                  |
                  +-- Return its result
                  +-- Populate cache only if backfill is enabled
```

For reactive `webclient` methods, these actions should remain in the reactive chain so merely constructing a `Mono` or `Flux` does not perform network work. The exact handling of streaming responses remains to be decided.

With **backfill disabled**, the consumer never writes a fetched result to the cache. The data owning service is the only source of cache population. This prevents an in-flight consumer read of an old value from restoring that value after invalidation. A miss can continue to go to the original service, at the cost of latency.

### Freshness rule and owner update sequence

The intended rule is: a read made while an update is in progress may observe the old value, as a database read can before commit. Once the update is reported complete, a new read should not receive the old cached value. A short interval while the update is being processed is acceptable.

For owner-managed population, the proposed sequence is:

1. Invalidate the affected cache entry or entries, waiting for confirmation from every cache node eligible to serve reads.
2. Change and commit the owning service's database record.
3. Optionally publish the new value to the cache. If this step fails, the cache should remain empty for that key and consumers should use the original service.
4. Report the update as complete.

If pre-update invalidation cannot be confirmed, the owner must not proceed under this freshness rule. A failed database update after invalidation merely leaves a cache miss. Concurrent updates require versions or another ordering mechanism so delayed messages cannot restore older values.

### Kubernetes cache-node coordination

The current preference is an **all-active, fully replicated cache**: every node can serve reads locally and can receive an invalidation or value update. This avoids choosing a master or assigning each key to a particular node. It also means every node needs capacity for the full cache, and each update requires one message per node.

- Applications address the cache through a stable Kubernetes Service.
- Cache nodes keep an in-memory membership list and broadcast invalidations or updates directly to the members. A normal Kubernetes Service request reaches one node; it is not itself a broadcast.
- Kubernetes maintains EndpointSlices as pods become ready, leave, or restart. The cache process must watch those changes and update its membership list; Kubernetes does not push application-level messages automatically. A watch requires suitable read permission and reconnect/resync handling.
- A broadcast completes only after every node eligible to serve reads acknowledges it. Membership changes during a broadcast need a defined retry or membership-epoch rule.
- A newly started node has an empty cache and must not serve cached reads until its membership/readiness state is safe. A node that cannot acknowledge an invalidation must be removed from the read pool before the owner update can complete, or the owner update must fail.
- Cache messages should be idempotent and carry a version or sequence so late delivery cannot overwrite newer state.

The full replication approach is intended for a modest node count. Its memory and broadcast cost grow with the number of nodes.

## Open decisions

1. **Generated-client compatibility:** Which OpenAPI Generator version, options, and generated method shapes will be supported? A representative generated `*Api.java` file will let us verify subclassing and reactive return types.
2. **Owner integration point:** How will the generated owner-mode code attach to successful database changes, and how will it receive the affected entity IDs and new values?
3. **Failure policy:** How exactly are unreachable cache nodes removed from the read pool, and what happens if membership cannot be established? Cache reads should fail as misses rather than return potentially stale data.
4. **Membership and restart protocol:** Define the watch/resync behavior, membership epochs, pod readiness, and handling of broadcasts concurrent with pod changes.
5. **Update ordering:** Choose a version source and conflict rule for concurrent writes to the same data.
6. **Population after restart:** With backfill disabled, restarted nodes begin empty. Decide whether owner services repopulate them, whether the cache stays sparse until subsequent writes, or whether another warmup mechanism is needed.
7. **Data handling:** Define serialization, maximum entry size, memory limits and eviction, namespace isolation, authentication, and treatment of caller-specific responses.
