# Getting started

dzdcache can run inside your service or as a separate cache cluster. What you add to your project, and how your code talks to the cache, differs between the two:

| | Embedded | Server |
| --- | --- | --- |
| Add to your build | The `dzdcache-spring-boot-starter` dependency | Nothing: you run the `ghcr.io/zdcache/dzdcache-server` container |
| Your code talks to | `DistributedCache` and `ClusterCache`, Spring beans in your own JVM | The HTTP API of any server node, from any language |
| Cached values | Your own Java objects | JSON |
| Who forms the cluster | The instances of your service | The server containers |

A third way combines them: a service adds the starter as a [learner](#a-learner-in-your-service) of a server cluster and searches a local copy of the server's data.

The examples cache students:

```java
public record Student(long id, String name, String city, int age) { }
```

## Embedded in a Spring Boot service

### 1. Add the dependency

```xml
<dependency>
  <groupId>io.github.zdcache</groupId>
  <artifactId>dzdcache-spring-boot-starter</artifactId>
  <version>0.1.1</version>
</dependency>
```

```kotlin
implementation("io.github.zdcache:dzdcache-spring-boot-starter:0.1.1")
```

The starter needs Java 21 and Spring Boot 4.1. It brings in [zdcache](https://github.com/zdcache/zdcache), whose `SearchSpecification` declares what can be searched.

### 2. Declare what to cache

Each `CacheType` bean is one replicated cache. Its specification names the identity and the properties you search by:

```java
@Bean
CacheType<Student, Long> students() {
    return CacheType.of("students", 1, Long.class,
            SearchSpecification.builder(IdentityDimension.of(Student.class, "id", Student::id))
                .exact(Student::city).prefix(Student::name).range(Student::age).build())
        .capacity(10_000)
        .ttl(Duration.ofMinutes(30));
}
```

The name and version identify the type to every instance; give it a new version when the class or the specification changes.

### 3. Use it from your code

The starter registers a `DistributedCache` bean. Ask it for the `ClusterCache` of your type and call it like a local cache:

```java
@Service
class StudentDirectory {
    private final ClusterCache<Student, Long> students;

    StudentDirectory(DistributedCache cache) {
        this.students = cache.cache(Student.class, Long.class);
    }

    void save(Student student) { students.put(student); }
    Student find(long id) { return students.get(id); }
    List<Student> livingIn(String city) { return students.search(Student::city, city); }
    List<Student> aged(int from, int to) { return students.search(Student::age, ValueRange.between(from, to)); }
    void forget(long id) { students.invalidate(id); }
}
```

- `get` and `search` read the copy in your own JVM and return the cached objects themselves, so cache immutable types such as records. `get` returns `null` for an id that is not cached.
- `put` and `invalidate` return once every instance that may serve reads has the change, so the next read on any instance sees it.
- While an instance cannot confirm it has every acknowledged write, as during a failover, a read waits up to three seconds and then throws `CacheException`; your code decides what to do without the cache.

### 4. Run it

With no settings, an instance runs standalone, so the service starts and its tests run without a cluster. To share the cache between instances, give each one a cluster token and the addresses of the others:

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

The instances find each other on port 7070, elect a master and copy the cache to every instance. [Configuration](CONFIGURATION.md) covers Kubernetes, two instances, TLS and the other settings.

## Server: a cache cluster your services call

### 1. Run the server

```bash
docker run -p 8080:8080 ghcr.io/zdcache/dzdcache-server:0.1.1
```

This is one standalone node without authentication, for trying it out. Set `DZDCACHE_API_TOKEN` to require a bearer token. For three nodes, see `docker compose up` in the [server README](../dzdcache-server/README.md#run-three-nodes-locally) and [k8s/dzdcache.yaml](../k8s/dzdcache.yaml) for Kubernetes.

### 2. Add nothing to your build

There is no client library: use the HTTP client you already have. The API is described in [openapi.yaml](../openapi.yaml), from which you can also generate a client. The examples below use Spring's `RestClient`, and Jackson converts the records to and from JSON:

```java
RestClient cache = RestClient.builder()
    .baseUrl("http://dzdcache:8080")
    .defaultHeader(HttpHeaders.AUTHORIZATION, "Bearer " + apiToken)
    .defaultHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
    .build();
```

### 3. Register the type once

A type is a JSON definition: its id and version, where the identity is, and the dimensions it can be searched by. Publishing the same definition again is harmless, so a service can publish it when it starts:

```java
cache.post().uri("/cache/schemas").body("""
    {"id": "students", "version": 1, "identity": "/id", "capacity": 10000,
     "dimensions": [
       {"name": "city", "path": "/city", "index": "exact", "type": "string"},
       {"name": "name", "path": "/name", "index": "prefix", "type": "string"},
       {"name": "age", "path": "/age", "index": "range", "type": "integer"}]}
    """).retrieve().toBodilessEntity();
```

A definition can also carry a JSON Schema and a TTL; see [schemas and TTL](../dzdcache-server/README.md#schemas-and-ttl).

### 4. Write, get and search

```java
cache.post().uri("/cache/entries")
    .body(Map.of("specification", "students", "schemaVersion", 1, "value", new Student(1, "Anna", "Oslo", 22)))
    .retrieve().toBodilessEntity();

Student anna = cache.post().uri("/cache/entries/get")
    .body(Map.of("specification", "students", "schemaVersion", 1, "identity", 1))
    .retrieve().body(Student.class);

List<Student> inOslo = cache.post().uri("/cache/search")
    .body(Map.of("specification", "students", "schemaVersion", 1, "dimension", "city", "value", "Oslo"))
    .retrieve().body(new ParameterizedTypeReference<List<Student>>() { });

List<Student> young = cache.post().uri("/cache/search")
    .body(Map.of("specification", "students", "schemaVersion", 1, "dimension", "age", "from", 18, "to", 25))
    .retrieve().body(new ParameterizedTypeReference<List<Student>>() { });
```

- A write returns once every node that may serve reads has it, so a read on any node sees it.
- A search returns a JSON array of matching values; an empty array is an answer, not a miss.
- A missing id is 404 (`HttpClientErrorException.NotFound` with `RestClient`), and 503 means the node cannot answer fresh right now. A failed write may already have been applied; send it again unchanged.

The [server README](../dzdcache-server/README.md#api) lists the endpoints, such as batches and invalidation, and [openapi.yaml](../openapi.yaml) has every request field, including search filters and the change stream.

## A learner in your service

A Spring Boot service can follow a server cluster as a learner: it keeps a full copy of the server's data and searches it in its own JVM, while writes still go to the servers over HTTP. This is new since 0.1.1 and comes with the next release.

```yaml
dzdcache:
  role: learner
  cluster-token: ${DZDCACHE_CLUSTER_TOKEN}
  discovery:
    mode: static
    self-url: http://10.0.0.21:7070
    static-peers:
      - http://10.0.0.11:7070
```

```java
JsonCache students = distributedCache.json("students", 1);
JsonNode anna = students.get(1);
List<JsonNode> inOslo = students.search("city", "Oslo");
```

A learner reads types registered without a JSON Schema, as above, and returns their JSON. [Configuration](CONFIGURATION.md#a-learner-in-your-service) explains its settings.

## Next

- [Configuration](CONFIGURATION.md): the settings for each way of running dzdcache, and why you would choose them.
- [Consistency and failure behavior](CONSISTENCY.md): what a read is guaranteed to see, and what happens when nodes fail.
