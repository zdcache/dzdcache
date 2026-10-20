# Distributed ZDCache (DZDCache)

Replicated, searchable in-memory caches built on [ZDCache](https://github.com/zdcache/zdcache). Every node holds a full replica and answers gets and searches locally; one elected master orders all writes in a replicated log, and a write is acknowledged once every node that may serve reads has applied it, so a read on any node sees every acknowledged write. Every node evicts the same roots. Kubernetes or a static peer list is only used while a node starts, to find the cluster.

This is an initial implementation. Reads are fresh on any node, with one exception: a two-node cluster without a witness whose link fails while both nodes stay up. See [consistency and failure behavior](docs/CONSISTENCY.md).

## Modules

| Module | Use it when |
| --- | --- |
| [dzdcache-spring-boot-starter](dzdcache-spring-boot-starter/README.md) | A Spring Boot service caches its own Java objects, and its instances share the cache among themselves. |
| [dzdcache-server](dzdcache-server/README.md) | A central cache cluster that many services publish JSON to and query over HTTP, with types registered at runtime as JSON Schema. Runs as a container. |
| [dzdcache-core](dzdcache-core/README.md) | Embedding without Spring, or building another integration. The starter and the server are built on it. |

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
