# dzdcache-spring-boot-starter

Embeds a replicated [ZDCache](https://github.com/zdcache/zdcache) in a Spring Boot service. The instances of the service form a cluster among themselves: each one holds a full replica of the service's own Java objects, searches them locally, and sends writes to one elected master, which replicates them to every instance.

## Dependency

```xml
<dependency>
  <groupId>io.github.zdcache</groupId>
  <artifactId>dzdcache-spring-boot-starter</artifactId>
  <version>${dzdcache.version}</version>
</dependency>
```

Requires Java 21 and Spring Boot 4.1. The service needs no web stack: cluster traffic uses its own port.

## Declare a type

Each `CacheType` bean is one replicated cache. The search specification is a zdcache `SearchSpecification`, and its identity must be read from the object.

```java
public record Student(long id, String name, String city, int age) { }

@Bean
CacheType<Student, Long> students() {
    return CacheType.of("students", 1, Long.class,
            SearchSpecification.builder(IdentityDimension.of(Student.class, "id", Student::id))
                .exact(Student::city).prefix(Student::name).range(Student::age).build())
        .capacity(10_000)
        .ttl(Duration.ofMinutes(30));
}
```

Capacity defaults to 10,000 roots and TTL to infinite.

## Use it

```java
ClusterCache<Student, Long> students = distributedCache.cache(Student.class, Long.class);

students.put(new Student(1, "Anna", "Oslo", 22));
Student anna = students.get(1L);
List<Student> inOslo = students.search(Student::city, "Oslo");
List<Student> young = students.search("age", ValueRange.between(18, 25));
students.invalidate(1L);
```

- `put`, `putAll`, `invalidate` and `evict` return once the master and every in-sync instance have the change. `putAsync` returns a `CompletableFuture`.
- Without a revision, a write gets the next revision for its identity and the last write wins. Pass an explicit revision to `put(value, revision)` or `invalidate(id, revision)` when the data owner orders its changes; older revisions are then ignored.
- `get` and `search` read the local replica. A `get` counts as use for the LRU on every instance; a search does not.
- Reads and writes throw `CacheException` while the instance has not joined the cluster. `DistributedCache.ready()` reports whether it has.

Values are converted with the service's `ObjectMapper` when there is exactly one; cluster messages use a private mapper.

## Configuration

| Setting | Default |
| --- | --- |
| `dzdcache.enabled` | `true` |
| `dzdcache.discovery.mode` | `standalone`; also `static` or `kubernetes` |
| `dzdcache.discovery.node-id` | random UUID |
| `dzdcache.discovery.self-url` | `http://localhost:7070`, the URL other instances use for this one |
| `dzdcache.discovery.static-peers` | empty |
| `dzdcache.discovery.namespace`, `dzdcache.discovery.service-name` | `default`, `dzdcache-peers` |
| `dzdcache.cluster-port` | 7070 |
| `dzdcache.cluster-token` | required unless standalone |

The other `dzdcache.*` settings and their defaults are the same as for the [server](../dzdcache-server/README.md#operational-bounds).

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

In `kubernetes` mode a starting instance lists the EndpointSlices of a headless peer Service with `publishNotReadyAddresses: true`, and needs a Role that permits EndpointSlice list; see [the server's manifest](../k8s/dzdcache.yaml) for an example. After joining, the Kubernetes API is not used.

## Rolling upgrades

Every instance publishes its declared types when it starts. A type's fingerprint covers its class, properties, identity, dimensions, capacity and TTL. An instance that does not declare a type, or declares it with a different fingerprint, skips that type's changes and lists it as unavailable in health. Give a type a new version when its class or specification changes, so old and new instances can run side by side.

## Health and metrics

With Actuator present, a `clusterHealthIndicator` reports the role, master, members and unavailable types. To gate readiness on joining:

```yaml
management.endpoint.health.group.readiness.include: readinessState,cluster
```

Metrics are registered in the service's `MeterRegistry` when there is one.

## Limits

Every instance holds a full replica, so this is intended for modest cache sizes and instance counts. Nothing is persisted: when all instances stop, the cache is empty. See [consistency and failure behavior](../docs/CONSISTENCY.md).
