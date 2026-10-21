# dzdcache-server

A Java 21 / Spring Boot service around [ZDCache](https://github.com/zdcache/zdcache). Publish a schema and search dimensions, send JSON root graphs, and query them through one generic HTTP API. Each node compiles the published schema into Java classes at runtime and stores those objects in ZDCache. Domain model JARs are unnecessary.

Commands in this file run from the repository root.

## Implemented

- Runtime schema publication, validation, in-memory Java compilation, and isolated classes per schema version.
- ZDCache identity, exact, prefix, range, and nested-collection indexes; full root graph serialization.
- Single-root and batch writes, identity lookup, search, invalidation, and explicit eviction.
- Infinite or finite TTL, local LRU eviction, per-root revisions, conflict detection, and deletion tombstones.
- Replication through one elected master: a replicated log, read leases, acknowledgment once every node that may serve reads has applied a write, catch-up from the log or a snapshot, and voted failover.
- Standalone, configured-peer, and Kubernetes EndpointSlice discovery while a node starts.
- Separate API/peer bearer tokens, payload limits, readiness/liveness probes, Docker and Kubernetes configuration.

This is an initial implementation. Its distribution protocol provides one order of changes, and a read on any node returns every write acknowledged before it started, or 503. **The exception is a two-node cluster without a witness whose link fails while both nodes stay up.** See [consistency and failure behavior](../docs/CONSISTENCY.md) before connecting owner updates.

## Responsibilities

| Component | Responsibility |
| --- | --- |
| Publishing service | Register schemas, identities, search dimensions, capacity, and TTL; populate roots; publish updates or invalidations with ordered revisions; arrange repopulation after a complete cache-cluster restart. |
| DZDCache | Validate schemas and graphs, compile runtime types, store and index roots, answer queries, enforce TTL/LRU, and replicate schemas and mutations among nodes. |
| Consuming service | Choose the specification/version and search to use, interpret the response, and decide where and how to obtain data when the cache cannot satisfy its needs. |

Any authorized service can publish objects. Each root needs a clear authority for its contents and revision ordering, usually the data owner or a designated publisher acting for it. Publishers send requests to DZDCache's normal service address; the receiving node forwards writes to the master, which replicates them. Publishers do not maintain a pod list.

Publishers and consumers agree on specification ids, versions, identities, and search dimensions. Definitions can be inspected through `GET /cache/schemas` and `GET /cache/schemas/{id}/{version}`. Schema publication must precede writes and queries for that version.

DZDCache owns no original-service URLs, fallback rules, or business-data loading workflow. A missing identity is reported as 404, and an unavailable cache as 503. A successful search, including `200 []`, is the answer for matching roots currently in the cache; an empty array is not a cache miss. Consumers own any calls to other data sources. A service that also publishes fetched data takes on the publisher's revision and update responsibilities.

OpenAPI-generated client inspection and cache integration generation are optional external tooling, outside the DZDCache service scope. Services can use WebClient or another HTTP client directly. A shared client library could simplify the cache API calls while leaving data-source selection and fallback policy with each consumer. Runtime schema compilation remains part of DZDCache and requires no client-code generator.

## Build and run

Requirements: **JDK 21** (including the compiler) and curl for the examples.

```bash
./gradlew build
java -jar dzdcache-server/build/libs/dzdcache-server.jar
```

The default is one standalone node on port 8080. The API is unauthenticated in that local default; set `DZDCACHE_API_TOKEN` to require `Authorization: Bearer <token>`. Distributed mode also requires a shared `DZDCACHE_CLUSTER_TOKEN`. The Kubernetes manifest requires both tokens from a Secret.

The build pins Spring Boot 4.1.1 and the published Maven Central artifact `io.github.zdcache:zdcache:0.3.1`.

Run `bash scripts/smoke-test.sh` after packaging to exercise **the executable JAR**, including runtime compilation. `scripts/soak/soak.sh` runs three local nodes under the k6 load for `DURATION_SECONDS`, kills nodes and cuts their inbound cluster traffic one at a time, and fails if any acknowledged write was missing on a node that answered or if the nodes differ at the end. `scripts/benchmark.sh` runs the in-process JMH benchmarks and passes its arguments to JMH; `./gradlew :dzdcache-server:footprint` prints the retained heap per cached root for each type kind with 100,000 student roots (`-Pfootprint.roots=N` changes the count); `scripts/load/search-with-writes.js` is a k6 load test of three users searching and getting by identity and one writer against a running node or cluster, configured with `BASE_URL`, `TOKEN`, `SIZE`, `CAPACITY` and `DURATION`; with `READ_URLS`, a comma-separated list of other nodes, the writer also reads each acknowledged write back from them and checks that it is visible at once. Tests also start two real HTTP nodes and a simulated Kubernetes API. They do not need Docker or a Kubernetes cluster.

## Try a schema and object graph

The examples assume the standalone default with no token. Add an authorization header if configured.

```bash
curl --fail-with-body -H 'Content-Type: application/json' \
  localhost:8080/cache/schemas --data-binary @examples/student-schema.json

curl --fail-with-body -H 'Content-Type: application/json' \
  localhost:8080/cache/entries --data-binary @examples/put-student.json

curl --fail-with-body -H 'Content-Type: application/json' localhost:8080/cache/search \
  -d '{"specification":"students","schemaVersion":1,"dimension":"sex","value":"female"}'
```

The result contains Anna's complete root graph, including her address and courses. A search selects roots; it does not prune the matching roots' children. No match is **`200 []`**, and consumers must accept that as the answer. An empty or partially populated cache can therefore produce an empty or partial collection. The service makes no claim that it contains every database row.

```bash
# Inclusive numeric range
curl -H 'Content-Type: application/json' localhost:8080/cache/search \
  -d '{"specification":"students","schemaVersion":1,"dimension":"age","from":18,"to":25}'

# Exact search across a root's nested course collection
curl -H 'Content-Type: application/json' localhost:8080/cache/search \
  -d '{"specification":"students","schemaVersion":1,"dimension":"course","value":"CS101"}'

# A single absent root returns 404
curl -H 'Content-Type: application/json' localhost:8080/cache/entries/get \
  -d '{"specification":"students","schemaVersion":1,"identity":42}'

# A newer invalidation fences late revision-1 writes
curl -H 'Content-Type: application/json' localhost:8080/cache/entries/invalidate \
  -d '{"specification":"students","schemaVersion":1,"identity":42,"revision":2}'
```

For a collection write, send an array of entry request objects to `/cache/entries/batch`. Each entry is cached separately and receives its own outcome. Batch operations are not atomic; inspect every item's `status` even when the HTTP response is 200.

## Schemas and TTL

See [student-schema.json](../examples/student-schema.json) and [schema contracts](../docs/SCHEMAS.md).

| Field | Meaning |
| --- | --- |
| `id`, `version` | Immutable logical contract; different versions have separate caches. |
| `schema` | Optional. Supported JSON Schema object description, including nested objects and arrays. Without it, every dimension declares a `type` (`string`, `integer`, `number`, `boolean`, `timestamp`); see [student-json.json](../examples/student-json.json). |
| `identity` | JSON Pointer to one non-null scalar identity, such as `/id`. |
| `dimensions` | Named `exact`, `prefix`, or inclusive `range` searches. `*` traverses arrays. |
| `capacity` | Maximum live root count in the local ZDCache instance. |
| `ttl` | `infinite` (also the default) or a positive ISO-8601 duration such as `PT5M`. |

Finite TTL writes supply a stable **`writtenAt` epoch-millisecond timestamp**, typically attached to the owner's change event. The service computes `writtenAt + ttl`. Alternatively, supply an absolute `expiresAt` deadline. Reuse the same value on retries; a retry must not extend TTL. Infinite TTL needs neither field. An explicit `expiresAt` overrides the schema TTL for that entry. Nodes replicate the absolute deadline and need synchronized clocks. Expiry is enforced before reads and snapshots.

Schema publication is explicit. This version does not infer a reliable schema from sample JSON, resolve remote references, or accept Java source uploads. Reusing a schema version with different content returns 409; publish a new version for changes.

## API

All entry and query paths are generic. The specification and version travel in the body. Full request schemas are in [openapi.yaml](../openapi.yaml).

| Method/path | Result |
| --- | --- |
| `POST /cache/schemas` | Publish and replicate a schema; 201, including idempotent republication. |
| `GET /cache/schemas` | List published definitions. |
| `GET /cache/schemas/{id}/{version}` | Inspect a definition and its search dimensions. |
| `POST /cache/entries` | Put a root; 200 once it has committed and every node that may serve reads has applied it. |
| `POST /cache/entries/batch` | Array of per-root outcomes; 200 for the batch envelope. |
| `POST /cache/entries/get` | Root JSON, or 404 for an absent root. `fields` works as for search. |
| `POST /cache/search` | Matching root graphs; 200 even when empty. An optional `limit` caps the count, and `fields` selects JSON Pointers to return instead of whole graphs. |
| `POST /cache/entries/invalidate` | Newer revision tombstone; 204. |
| `POST /cache/entries/evict` | Remove a matching revision on every member; 204. |
| `GET /cache/cluster` | Size, members, master, and the previous size and members while a resize runs. |
| `PUT /cache/cluster` | Resize while running: `{"size":5}`, or `{"size":3,"leaving":["dzdcache-3","dzdcache-4"]}` to shrink; 200 once the change is complete, 409 while another resize runs. |
| `GET /actuator/health/readiness` | 200 when this node can serve fresh reads; 503 while joining or without a read lease. |
| `GET /actuator/health/liveness` | Process health, independent of peer reachability. |
| `GET /actuator/prometheus` | Metrics: search and get timers, search result sizes, mutation, election, snapshot and replication-failure counts, readiness, whether the node serves fresh reads, refused reads by reason, master, term, members, reachable members and configured size, replication lag per member, two-node reconciles, startup problems, applied log index, cached roots and revision records per schema. [monitoring/](../monitoring) has Prometheus alert rules and a Grafana dashboard. |

Invalid input returns 400; incompatible schemas or equal-revision conflicts return 409; schema/revision metadata limits return 429; oversized requests return 413; searches matching more than `max-search-results` roots without a `limit` return 422; availability/replication failures return 503. Unknown specifications return 404. A failed distributed mutation **may already have applied** to some nodes: retry its unchanged revision and payload. Older mutations are ignored and can still receive a successful response.

Nodes talk to each other on a separate cluster port, `dzdcache.cluster-port` (7070), over one TCP connection per peer: log entries, votes, snapshots, joins and forwarded writes. It uses the cluster token and remains available while a node joins. A standalone node does not open it. Every connection starts with the cluster protocol version; a node that speaks another version is refused, so a rolling update to an incompatible version stops at the first new node instead of mixing versions.

## Run three nodes locally

```bash
export DZDCACHE_API_TOKEN="$(openssl rand -hex 32)"
export DZDCACHE_CLUSTER_TOKEN="$(openssl rand -hex 32)"
docker compose up --build
```

Nodes listen on host ports 8081, 8082 and 8083. Publish/write through any of them and read through another using the API token. Readiness becomes healthy once a node has joined the cluster and holds a read lease. Stopping one node leaves the other two to elect a master if needed and keep accepting writes.

## Kubernetes

Build and push the image to your registry, then set its reference in [k8s/dzdcache.yaml](../k8s/dzdcache.yaml). For a local cluster, loading `dzdcache:local` into that cluster's image store also works.

```bash
kubectl create secret generic dzdcache-auth \
  --from-literal=api-token="$(openssl rand -hex 32)" \
  --from-literal=cluster-token="$(openssl rand -hex 32)"
kubectl apply -f k8s/dzdcache.yaml
```

Apply the Secret and manifests to the same namespace. `scripts/k8s-test.sh` runs the manifest on a local [kind](https://kind.sigs.k8s.io) cluster and checks a hard kill of the master pod, a rolling restart, and a live resize from three pods to five and back. The namespaced Role permits EndpointSlice list. A starting node lists the peer Service's EndpointSlices to find the cluster; once it has joined, the master keeps the membership and the Kubernetes API is not used.

- **`dzdcache:8080`** routes clients to ready pods.
- **`dzdcache-peers`** is a separate headless Service with `publishNotReadyAddresses: true`. Joining nodes must be reachable for replication before becoming client-ready; readiness-filtered peers would create a bootstrap cycle.
- The pods form a StatefulSet. Each pod's name is its node id, so a restarted pod takes back its slot, and `DZDCACHE_CLUSTER_SIZE` must equal `replicas`. The downward-API pod IP and UID allow self-exclusion.
- Pods are created and replaced one at a time. A starting pod that finds no cluster reports ready so that the next pod can start, and the first two found the cluster together; a pod that rejoins an existing cluster reports ready only once it serves fresh reads, so a rolling restart never takes down more than one node. To resize, call `PUT /cache/cluster` and scale the StatefulSet: to grow, `{"size":5}` then `kubectl scale statefulset dzdcache --replicas=5`; to shrink, `{"size":3,"leaving":["dzdcache-3","dzdcache-4"]}` then `--replicas=3`, since the StatefulSet removes the highest ordinals. Update `DZDCACHE_CLUSTER_SIZE` afterwards for the next full start; the rolling restart that triggers is safe.
- [k8s/dzdcache-learners.yaml](../k8s/dzdcache-learners.yaml) adds read replicas: a Deployment of learners that find the members through `dzdcache-peers` and serve clients through their own `dzdcache-learners` Service. Scale it freely; learners need no slot, no volume and no resize.
- Probes separate cluster failure from process failure. Pods use a full JDK, run as non-root, and have a read-only filesystem with writable `/tmp`.

The manifest does not install TLS or namespace authorization. Bearer tokens assume a trusted private network; use network policies and TLS/mTLS through your platform when needed. TLS on the cluster port itself is optional and off by default; see [Cluster TLS](../dzdcache-spring-boot-starter/README.md#cluster-tls). The shared API token grants all cache operations; there are no per-tenant ACLs. Building this project does not deploy it.

## Operational bounds

| Setting | Default |
| --- | --- |
| `dzdcache.max-schemas` | 100 |
| `dzdcache.max-revisions-per-schema` | 100,000 distinct identities |
| `dzdcache.max-request-bytes` | 8 MiB |
| `dzdcache.max-snapshot-bytes` | 64 MiB |
| `dzdcache.replication-page-bytes` | 1 MiB |
| `dzdcache.max-search-results` | 10,000 |
| `dzdcache.max-scan` | 100,000; the most roots a search with `scan` may cover before it returns 422 |
| `dzdcache.revision-retention` | 24 hours |
| `dzdcache.access-flush-interval` | 1 second |
| `dzdcache.replication-timeout` | 3 seconds |
| `dzdcache.sync-interval` | 10 seconds |
| `dzdcache.cluster-port` | 7070 |
| `dzdcache.changes-buffer` | 100,000; recent changes kept for subscribers that resume; an older position gets `RESET` |
| `dzdcache.cluster-transport` | `tcp`; framed messages on `cluster-port`, the fastest. `websocket` sends cluster traffic over a WebSocket to `/internal/raft/ws` on the application's own servlet web server, and `http2` over HTTP/2 to `/internal/raft`, which needs `server.http2.enabled` (`DZDCACHE_HTTP2` in the server, off by default because it slows HTTP/1.1 clients); both are for networks that only let HTTP through, `websocket` is the faster of the two, and peer URLs then use the application port, which cluster and API traffic share. Every node must use the same transport |
| `dzdcache.cluster-encoding` | `json`; `smile` encodes cluster messages in binary Smile instead. Every node must use the same encoding |
| `dzdcache.cluster-size` | the size a new cluster is founded with; static: the peer list plus this node; Kubernetes: required, equal to `replicas` |
| `dzdcache.heartbeat-interval` | 200 milliseconds |
| `dzdcache.election-timeout` | 1 second |
| `dzdcache.read-lease` | 500 milliseconds |
| `dzdcache.read-wait` | 3 seconds; how long a read waits for its node to become current before answering 503 |
| `dzdcache.max-staleness` | 0, never answer stale; above 0, a node that cannot confirm it has every acknowledged write answers from its own copy if that was current at most this long ago, and says so (`X-Dzdcache-Stale`, `X-Dzdcache-Staleness-Ms` in the server); it then also stays ready |
| `dzdcache.role` | `member`; `learner` makes the node a non-voting replica: it holds a full copy, applies the log like a member, forwards writes to the master and never founds a cluster, votes or stands for election, so it adds read capacity without enlarging the quorum. A learner joins without a slot, a learner that stays silent for `member-timeout` is removed and rejoins when it is back, and its peer list only has to reach the members |
| `dzdcache.learner-fresh-reads` | `true`; a learner holds a read lease like a follower, so its reads are fresh and the master waits for it to apply a write before acknowledging it. `false` takes no lease: writes do not wait for the learner, which then only answers stale within `max-staleness` (required above 0); for learners far from the members |
| `dzdcache.max-learners` | 16; the most learners the master accepts |
| `dzdcache.two-node-takeover` | 3 seconds; not used with a witness |
| `dzdcache.witness-url` | none; the cluster URL of a witness for a two-node cluster |
| `dzdcache.member-timeout` | 30 seconds |
| `dzdcache.log-retention-bytes` | 64 MiB |
| `dzdcache.cluster-tls.enabled` | `false`; TLS on the cluster port |
| `dzdcache.cluster-tls.bundle` | required when enabled; a `spring.ssl.bundle.*` name |
| `dzdcache.cluster-tls.client-auth` | `none`; `need` requires trusted client certificates (mTLS) |
| `dzdcache.cluster-tls.verify-hostname` | `true`; certificates then need IP SANs for pod IPs |
| `dzdcache.persistence.enabled` | `false`; `DZDCACHE_PERSISTENCE_ENABLED`; keeps the log, term and vote on disk |
| `dzdcache.persistence.directory` | `/var/lib/dzdcache`; `DZDCACHE_PERSISTENCE_DIRECTORY` |
| `dzdcache.persistence.segment-bytes` | 64 MiB; size of one journal file |
| `dzdcache.persistence.snapshot-bytes` | 64 MiB; journal growth after which a snapshot is written and older journal files are deleted |

Each node holds a full replica, and every node evicts the same roots: a root's last use is the latest of its writes and its gets by identity on any node, members report the gets they served to the master every `access-flush-interval`, the master logs them, and each node keeps the `capacity` most recently used roots, with identity order breaking ties. Searches do not count as use. Nodes agree once they have applied the same log entries. Capacity counts roots, not bytes. Revision fences/tombstones survive value eviction and are bounded separately; new identities are rejected at that bound rather than forgetting ordering protection. Tombstones are dropped once `revision-retention` has passed since their log entry, expired entries once it has passed since their deadline, and records of evicted roots once it has passed since their last use. A mutation older than a dropped record is accepted, so publishers must not deliver or retry mutations later than the retention. There is no schema deletion yet. Use deliberate version rollover and owner repopulation when needed; old namespaces still count toward the schema limit until restart.

A member that falls behind catches up from the master's log, which keeps up to `log-retention-bytes`, or installs a snapshot sent in pages of about `replication-page-bytes`. Every node holds a full replica, so this implementation is intended for modest cache sizes and node counts. Eviction listeners and durable schema/fence storage are follow-up work.

Without persistence nothing is written to disk. A surviving peer can refill a restarted node. If all nodes restart, the owner must republish schemas and roots; collection results are authoritative empty arrays once their schema is published until roots are loaded. Infinite TTL does not provide durability.

With `dzdcache.persistence.enabled`, each node keeps its log, term and vote in a journal under `dzdcache.persistence.directory`, and a write is acknowledged only once it is on the disks of a majority. Writes arriving together share one fsync. A restarted node keeps its slot and catches up from where its disk ends, and a cluster whose nodes all restart comes back with its schemas and data. A torn last journal record is cut off at startup; damage elsewhere stops the node, and its liveness probe then fails. In Kubernetes each pod gets its own volume (`volumeClaimTemplates` in `k8s/dzdcache.yaml`).

## Source layout

The server is built on [dzdcache-spring-boot-starter](../dzdcache-spring-boot-starter/README.md). `schema` is the bounded JSON Schema compiler and graph codec, and `api` and `config` hold the HTTP boundary, auth/size checks and configuration.

The [original discussion](../DESIGN.md), [earlier service proposal](../SINGLE_NODE_SERVICE.md), and [JSON adapter experiment](../experiments/json-root-adapter/README.md) remain as design history. The running implementation uses runtime-compiled objects and the generic API above.
