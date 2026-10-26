# Distributed ZDCache (DZDCache)

Replicated, searchable in-memory caches built on [ZDCache](https://github.com/zdcache/zdcache). Every node holds a full replica and answers gets and searches locally; one elected master orders all writes in a replicated log, and a write is acknowledged once every node that may serve reads has applied it, so a read on any node sees every acknowledged write. Every node evicts the same roots. Kubernetes or a static peer list is only used while a node starts, to find the cluster.

This is an initial implementation. Reads are fresh on any node, with one exception: a two-node cluster without a witness whose link fails while both nodes stay up. See [consistency and failure behavior](docs/CONSISTENCY.md).

## Modules

| Module | Use it when |
| --- | --- |
| [dzdcache-spring-boot-starter](dzdcache-spring-boot-starter/README.md) | A Spring Boot service caches its own Java objects, and its instances share the cache among themselves. |
| [dzdcache-server](dzdcache-server/README.md) | A central cache cluster that many services publish JSON to and query over HTTP, with types registered at runtime as JSON Schema. Runs as a container. |
| [dzdcache-core](dzdcache-core/README.md) | Embedding without Spring, or building another integration. The starter and the server are built on it. |

## Three ways to use it

[Getting started](docs/GETTING_STARTED.md) shows the code for each, and [Configuration](docs/CONFIGURATION.md) the settings and why you would choose them.

| | Embedded | Server cluster | Server cluster with a learner |
| --- | --- | --- | --- |
| Where the cache runs | Inside each instance of your Spring Boot service | In its own containers, which many services share | In the server containers, with a full copy inside your Spring Boot service |
| What is cached | The service's own Java objects, declared as `CacheType` beans | JSON, with types registered at runtime over HTTP | The server's JSON types |
| Reads and writes | Method calls on `ClusterCache` | HTTP to any server node | Reads on the local copy through `DistributedCache.json`; writes over HTTP to the servers |
| Module | starter | server | server, and the starter in your service |
| Benchmark group | in-process | out-of-process | not benchmarked |

### Embedded

Add [dzdcache-spring-boot-starter](dzdcache-spring-boot-starter/README.md) and declare a `CacheType` bean per type. The instances of the service find each other and form the cluster; there is nothing else to run. Reads are method calls on the local replica, with no network or serialization, which is why this is the fastest way to search.

```yaml
dzdcache:
  cluster-token: ${DZDCACHE_CLUSTER_TOKEN}
  discovery:
    mode: static
    self-url: http://10.0.0.11:7070
    static-peers:
      - http://10.0.0.12:7070
      - http://10.0.0.13:7070
```

- In Kubernetes, use `discovery.mode: kubernetes`, run the service as a StatefulSet with the pod name as `discovery.node-id`, and set `cluster-size` to the replica count.
- `ClusterCache.onChange` listeners receive every change without further settings. `changes-buffer` is 0, so nothing is kept for replay; set it only if a listener resumes from an earlier position with `onChange(from, …)`. Each buffered change holds a copy of its value.
- Nothing is persisted: when every instance stops, the cache is empty.

### Server cluster

Run [dzdcache-server](dzdcache-server/README.md) as a container, usually three nodes with [k8s/dzdcache.yaml](k8s/dzdcache.yaml). Services register types as JSON Schema or JSON-only definitions, publish JSON roots and search over HTTP, so they need no shared Java classes and can be written in any language. Every call pays for a network round trip and JSON serialization.

```bash
DZDCACHE_API_TOKEN=...            # clients send it as a bearer token
DZDCACHE_CLUSTER_TOKEN=...        # shared by the nodes
DZDCACHE_DISCOVERY_MODE=kubernetes
DZDCACHE_CLUSTER_SIZE=3           # equal to the StatefulSet's replicas
DZDCACHE_PERSISTENCE_ENABLED=true # optional: the data survives a full restart
```

- The server keeps the last 100,000 changes (`dzdcache.changes-buffer`), so a client of `GET /cache/changes` that reconnects with `Last-Event-ID` continues where it stopped, on any node.
- More read capacity without a larger quorum: add server learners with [k8s/dzdcache-learners.yaml](k8s/dzdcache-learners.yaml).

### Server cluster with a learner in your service

A Spring Boot service adds the starter as a learner of the server cluster. It holds a full copy, applies the servers' log, and reads the server's JSON-only types (registered without a `schema`) locally through `distributedCache.json("students", 1)`, with `get`, `search` and `count`. Writes still go to the servers over HTTP. Use this when one service searches far more often than a network round trip allows, while other services keep using the server.

```yaml
dzdcache:
  role: learner
  cluster-token: ${DZDCACHE_CLUSTER_TOKEN}   # the servers' cluster token
  discovery:
    mode: static
    self-url: http://10.0.0.21:7070          # reachable from the servers
    static-peers:                            # the servers' cluster ports
      - http://10.0.0.11:7070
      - http://10.0.0.12:7070
      - http://10.0.0.13:7070
```

- In the same Kubernetes namespace, `discovery.mode: kubernetes` with `discovery.service-name: dzdcache-peers` finds the servers instead of a peer list; the service then needs a Role that permits EndpointSlice list, as in the server's manifest.
- `cluster-transport` and `cluster-encoding` must match the servers (`tcp` and `json` by default).
- With `learner-fresh-reads: true`, the default, the learner's reads are fresh, and every write waits for it. A learner far from the servers can set it to `false` with `max-staleness` above 0: writes then do not wait for it, and it answers only from a copy at most that old.
- The servers accept up to `max-learners` (16) learners. Types registered with a JSON Schema need the server's runtime compiler, and a learner lists them as unavailable.

## Build

Requirements: **JDK 21** (including the compiler). The Gradle wrapper downloads Gradle.

```bash
./gradlew build
```

The build pins Spring Boot 4.1.1 and the published Maven Central artifact `io.github.zdcache:zdcache:0.3.1`.

The [original discussion](DESIGN.md), [earlier service proposal](SINGLE_NODE_SERVICE.md), and [JSON adapter experiment](experiments/json-root-adapter/README.md) remain as design history.

## Benchmark

`dzdcache-benchmark` compares dzdcache with Hazelcast, Apache Ignite, Infinispan and Redis Stack, each running three nodes. It uses the searches dzdcache is built for (people in a city, a name prefix, an age range, a city with an age range, and a tag in a nested collection), with lookups by id and writes for comparison. In-process caches, which run inside the application's JVM, are compared with each other; out-of-process services, the dzdcache server and Redis Stack, run in Docker and are compared with each other. Run `scripts/compare.sh -quick` for a short run or `scripts/compare.sh -full` on an idle machine; the report is written to `dzdcache-benchmark/build/reports/dzdcache-benchmark/report.md`, and `scripts/compare.sh -publish` publishes a full run.
<!-- benchmark-link -->
The latest published run is in the [benchmark report](docs/BENCHMARK.md).

Changes between versions are listed in the [changelog](CHANGELOG.md).

## Licence

[Apache License 2.0](LICENSE).
