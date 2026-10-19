# dzdcache-core

The replication engine behind dzdcache, without Spring. Most services should use [dzdcache-spring-boot-starter](../dzdcache-spring-boot-starter/README.md), which wires this module; depend on core directly only to embed dzdcache in a non-Spring application or to build another integration.

```xml
<dependency>
  <groupId>io.github.zdcache</groupId>
  <artifactId>dzdcache-core</artifactId>
  <version>${dzdcache.version}</version>
</dependency>
```

```kotlin
implementation("io.github.zdcache:dzdcache-core:$dzdcacheVersion")
```

Dependencies: zdcache, Jackson 3 databind, Micrometer core and SLF4J.

## Packages

| Package | Contents |
| --- | --- |
| `cache` | `CacheEngine`: ZDCache root storage per type and version, TTL, the shared LRU, revision fences and tombstones, snapshots. `RootType` is how a stored type reads, writes and indexes its roots; `TypeDescriptor` is what the cluster replicates about a type. |
| `typed` | `JacksonRootType`: a `RootType` for an application's own Java classes, indexed with a zdcache `SearchSpecification`. |
| `cluster` | `ClusterCoordinator`: elections, the replicated log, membership, catch-up and snapshot install. `HttpRaftTransport` and `ClusterServer` carry its messages over HTTP on the cluster port. |
| `discovery` | Startup peer lists: `StaticPeerDirectory` and `KubernetesPeerDirectory` (EndpointSlices). |
| `config` | `ServiceProperties`: limits and timings. |

## Wiring

An embedding builds a `CacheEngine` inside an `EngineHolder`, registers its local types, then creates a `PeerDirectory`, an `HttpRaftTransport`, a `ClusterCoordinator` and a `ClusterServer`, starts the directory, the server and the coordinator, and publishes its type descriptors once the coordinator is ready. `DzdCacheAutoConfiguration` and `DzdCacheLifecycle` in the starter do exactly this and are the reference. A type whose descriptor arrives through the log without a local registration is created by the engine's `RootTypeFactory`, or recorded as unavailable when there is none.

The rules the coordinator follows — master election, acknowledgment by the in-sync members, quorum with the oldest member as tie-break, two-node takeover, membership changes — are in [consistency and failure behavior](../docs/CONSISTENCY.md).

## Tests

`ClusterSimulationTest` runs whole clusters in virtual time with partitions, crashes and restarts, and checks that acknowledged writes survive and every node ends with the same contents.
