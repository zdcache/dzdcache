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

```kotlin
implementation("io.github.zdcache:dzdcache-spring-boot-starter:$dzdcacheVersion")
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

- `put`, `putAll`, `invalidate` and `evict` return once the change has committed and every instance that may serve reads has applied it, so a read on any instance sees it. `putAsync` returns a `CompletableFuture`.
- Without a revision, a write gets the next revision for its identity and the last write wins. Pass an explicit revision to `put(value, revision)` or `invalidate(id, revision)` when the data owner orders its changes; older revisions are then ignored.
- `get` and `search` read the local replica. A `get` counts as use for the LRU on every instance; a search does not.
- A read waits up to `dzdcache.read-wait` (3 seconds) while the instance cannot confirm it has every acknowledged write, as during a failover, then throws `CacheException` (503); the calling thread blocks meanwhile. Writes throw it when no master can be reached. `DistributedCache.current()` reports whether reads can be served, and `ready()` whether the instance is a member.

Values are converted with the service's `ObjectMapper` when there is exactly one; cluster messages use a private mapper.

## Configuration

| Setting | Default |
| --- | --- |
| `dzdcache.enabled` | `true` |
| `dzdcache.discovery.mode` | `standalone`; also `static` or `kubernetes` |
| `dzdcache.cluster-size` | the size a new cluster is founded with; static: the configured nodes; Kubernetes: required, equal to the replica count |
| `dzdcache.discovery.node-id` | static: the self-url's host:port; otherwise a random UUID |
| `dzdcache.discovery.self-url` | `http://localhost:7070`, the URL other instances use for this one |
| `dzdcache.discovery.static-peers` | empty |
| `dzdcache.discovery.namespace`, `dzdcache.discovery.service-name` | `default`, `dzdcache-peers` |
| `dzdcache.cluster-port` | 7070 |
| `dzdcache.max-staleness` | 0, never answer stale; above 0, a node that cannot confirm it has every acknowledged write answers from its own copy if that was current at most this long ago, and says so; `awaitCurrent()` on `ClusterCoordinator` reports it; it then also stays ready |
| `dzdcache.role` | `member`; `learner` makes the node a non-voting replica: it holds a full copy, applies the log like a member, forwards writes to the master and never founds a cluster, votes or stands for election, so it adds read capacity without enlarging the quorum. A learner joins without a slot, a learner that stays silent for `member-timeout` is removed and rejoins when it is back, and its peer list only has to reach the members |
| `dzdcache.learner-fresh-reads` | `true`; a learner holds a read lease like a follower, so its reads are fresh and the master waits for it to apply a write before acknowledging it. `false` takes no lease: writes do not wait for the learner, which then only answers stale within `max-staleness` (required above 0); for learners far from the members |
| `dzdcache.max-learners` | 16; the most learners the master accepts |
| `dzdcache.max-scan` | 100,000; the most roots a search with `scan` may cover before it returns 422 |
| `dzdcache.changes-buffer` | 100,000; recent changes kept for subscribers that resume; an older position gets `RESET` |
| `dzdcache.cluster-transport` | `tcp`; framed messages on `cluster-port`, the fastest. `websocket` sends cluster traffic over a WebSocket to `/internal/raft/ws` on the application's own servlet web server, and `http2` over HTTP/2 to `/internal/raft`, which needs `server.http2.enabled` (`DZDCACHE_HTTP2` in the server, off by default because it slows HTTP/1.1 clients); both are for networks that only let HTTP through, `websocket` is the faster of the two, and peer URLs then use the application port, which cluster and API traffic share. Every node must use the same transport |
| `dzdcache.cluster-encoding` | `json`; `smile` encodes cluster messages in binary Smile instead. Every node must use the same encoding |
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

In `kubernetes` mode a starting instance lists the EndpointSlices of a headless peer Service with `publishNotReadyAddresses: true`, and needs a Role that permits EndpointSlice list; see [the server's manifest](../k8s/dzdcache.yaml) for an example. Run the instances as a StatefulSet with the pod name as `dzdcache.discovery.node-id`, so a restarted pod keeps its id. After joining, the Kubernetes API is not used.

## Cluster TLS

Cluster traffic is plain HTTP unless you turn TLS on. TLS is optional and off by default, so networks that terminate or forbid TLS in their own way keep working unchanged.

| Setting | Default |
| --- | --- |
| `dzdcache.cluster-tls.enabled` | `false` |
| `dzdcache.cluster-tls.bundle` | required when enabled; the name of a Spring Boot SSL bundle (`spring.ssl.bundle.pem.<name>` or `spring.ssl.bundle.jks.<name>`) |
| `dzdcache.cluster-tls.client-auth` | `none`; `need` makes the cluster port require a client certificate trusted by the bundle's trust store, and instances present the bundle's key |
| `dzdcache.cluster-tls.verify-hostname` | `true` |

The bundle's key and certificate serve the cluster port, and its trust store decides which peers and clients are trusted. With TLS on, `self-url` and `static-peers` must use `https://` (and `http://` with it off); startup fails otherwise. Kubernetes discovery and the pod address advertised from `POD_IP` use `https://` automatically.

```yaml
dzdcache:
  cluster-tls:
    enabled: true
    bundle: cluster
    client-auth: need
spring:
  ssl:
    bundle:
      pem:
        cluster:
          keystore:
            certificate: file:/etc/dzdcache/tls/tls.crt
            private-key: file:/etc/dzdcache/tls/tls.key
          truststore:
            certificate: file:/etc/dzdcache/tls/ca.crt
```

Peers are usually addressed by IP, and Kubernetes peers always are. With `verify-hostname: true` every node's certificate needs a subject alternative name for each address peers use, such as `IP:10.0.0.11`; a DNS name alone does not match an IP address. When pod IPs are not known in advance, set `verify-hostname: false`: the certificate chain, validity and key usage are still checked against the bundle's trust store, but the address is not matched against the certificate, so use a private CA that only issues cluster certificates.

## Resizing

`DistributedCache.resize(5)` grows a running cluster; start the new instances afterwards and they join. `DistributedCache.resize(3, "node-3", "node-4")` shrinks it, naming the instances that leave; stop them once the call returns. The change goes through a joint configuration, so quorums stay safe while it runs, and `DistributedCache.view()` shows the size, members and any resize in progress.

## Two instances and a witness

A cluster of two instances can add a witness: a small process that stores no data and only decides which instance may act as master. Start the same application, or the dzdcache server, with `dzdcache.discovery.mode=witness`, `dzdcache.cluster-port` and the cluster token, and point both instances at it:

```yaml
dzdcache:
  witness-url: http://witness.internal:7070
```

With a witness, a broken link between the two instances leaves one master and loses no acknowledged write; without one, both act as master until the link returns. The witness is only for clusters of two.

## Rolling upgrades

Every instance publishes its declared types when it starts. A type's fingerprint covers its class, properties, identity, dimensions, capacity and TTL. An instance that does not declare a type, or declares it with a different fingerprint, skips that type's changes and lists it as unavailable in health. Give a type a new version when its class or specification changes, so old and new instances can run side by side.

## Health and metrics

With Actuator present, a `clusterHealthIndicator` reports the role, master, members, whether the instance is current, unavailable types and any startup problem, such as a self-url that reaches another node. To gate readiness on fresh reads:

```yaml
management.endpoint.health.group.readiness.include: readinessState,cluster
```

Metrics are registered in the service's `MeterRegistry` when there is one; [monitoring/](../monitoring) has Prometheus alert rules and a Grafana dashboard for them.

## Limits

Every instance holds a full replica, so this is intended for modest cache sizes and instance counts. Nothing is persisted: when all instances stop, the cache is empty. See [consistency and failure behavior](../docs/CONSISTENCY.md).
