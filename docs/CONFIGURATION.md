# Configuration

Every setting is a Spring Boot property under `dzdcache.`, with the same name in the starter and the server. Put it in `application.yaml`, or set it as an environment variable: `dzdcache.cluster-token` is `DZDCACHE_CLUSTER_TOKEN`, and `dzdcache.discovery.mode` is `DZDCACHE_DISCOVERY_MODE`. The defaults suit a single node; this page goes through the choices you make beyond that, what each one gives you and what it costs. [Getting started](GETTING_STARTED.md) shows the code that uses the cache.

## How many nodes

Every node holds a full copy of the cache and answers gets and searches from it. One elected master orders the writes, and a write is acknowledged once every node that may serve reads has applied it. A cluster keeps working while more than half of its configured nodes are up.

| Nodes | Survives | Use it for |
| --- | --- | --- |
| 1, standalone | Nothing; a restart empties the cache | Development, tests, and caches that can be refilled quickly. No cluster port is opened |
| 2 with a witness | One node failing, or the link between them breaking | Two instances, with a small witness process as the third vote |
| 2 without a witness | One node failing, but a broken link between them can lose writes | Only when losing some writes after a network split is acceptable |
| 3 | One node failing | Most clusters |
| 5 | Two nodes failing at once | Clusters that must survive a second failure, or a failure during maintenance |

More nodes give more read capacity, since every node answers reads on its own, but not more write capacity: a write waits until every node that may serve reads has it. Each node needs the memory for the whole cache.

### Finding each other

A node uses discovery only while it starts, to find the cluster; after that the master keeps the membership.

With a fixed set of machines, list them. Each node names its own address and the others':

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

In Kubernetes, run the nodes as a StatefulSet, so each pod keeps its name as its node id and takes back its own slot when it restarts. A starting node lists the EndpointSlices of a headless Service with `publishNotReadyAddresses: true`, which needs a Role that permits EndpointSlice list. [k8s/dzdcache.yaml](../k8s/dzdcache.yaml) has all of it for the server, and the starter takes the same settings:

```yaml
dzdcache:
  cluster-size: 3                         # equal to the StatefulSet's replicas
  cluster-token: ${DZDCACHE_CLUSTER_TOKEN}
  discovery:
    mode: kubernetes
    service-name: dzdcache-peers          # the headless peer Service
    namespace: ${POD_NAMESPACE}
    node-id: ${POD_NAME}
```

Set `POD_NAME` and `POD_NAMESPACE` from the pod's `metadata.name` and `metadata.namespace`, and `POD_IP` and `POD_UID` from `status.podIP` and `metadata.uid`, through the downward API: a node advertises `POD_IP` to the others and uses the UID to recognise itself.

`cluster-size` is only used to found a cluster. To grow or shrink a running one, call `PUT /cache/cluster` on the server or `DistributedCache.resize` in the starter; see [resizing](../dzdcache-spring-boot-starter/README.md#resizing).

### Two nodes and a witness

A witness stores no data and only decides which of two nodes may act as master. Without one, both nodes act as master when the link between them breaks, and the writes the losing node acknowledged meanwhile are discarded when they meet again. Start the starter or the server with `dzdcache.discovery.mode=witness`, the cluster port and the cluster token, and point both nodes at it:

```yaml
dzdcache:
  witness-url: http://witness.internal:7070
```

## Reads during a failover: fresh or available

When the master fails, the others elect a new one in about two to three seconds. Until then, a node that cannot confirm it has every acknowledged write does not answer.

| Setting | Default | What you get | What it costs |
| --- | --- | --- | --- |
| `read-wait` | 3 seconds | A read waits this long for its node to become current before it fails | Callers block meanwhile; a shorter wait fails faster |
| `max-staleness` | 0 | Above 0, a node that cannot confirm it is current answers from its own copy if that was current at most this long ago, labelled as stale, and stays ready | Answers can miss the most recent writes, by at most this much time |

Keep the default when an answer must include every acknowledged write. Set `max-staleness` when an answer a few seconds old is better than none: the server labels such answers with `X-Dzdcache-Stale` and `X-Dzdcache-Staleness-Ms`.

## More read capacity: learners

A learner is a node with `dzdcache.role: learner`. It holds a full copy and applies the same log, but it does not vote, so you can add learners without making elections or commits need more nodes. Learners need no slot and no resize: a learner joins with a peer list that reaches the members, and is removed when it stays silent for `member-timeout`.

| Setting | Default | Why |
| --- | --- | --- |
| `learner-fresh-reads` | `true` | The learner holds a read lease like a member, so its reads are fresh; every write then waits for it as well |
| `learner-fresh-reads: false` with `max-staleness` above 0 | – | Writes do not wait for the learner, which answers stale within `max-staleness`; for learners far from the members |
| `max-learners` | 16 | The most learners the master accepts |

Server learners add read capacity to a server cluster ([k8s/dzdcache-learners.yaml](../k8s/dzdcache-learners.yaml)). A Spring Boot service can also be a learner of a server cluster and search its data locally; see [Getting started](GETTING_STARTED.md#a-learner-in-your-service). Learners come with the release after 0.1.1.

## Surviving a full restart

Without persistence, the cache lives in memory: a node that restarts is refilled by the others, but when every node stops, the cache is empty and the owners of the data must publish it again.

The server can keep its log on disk:

| Setting | Default | |
| --- | --- | --- |
| `persistence.enabled` | `false` | `DZDCACHE_PERSISTENCE_ENABLED`; a write is acknowledged only once it is on the disks of a majority, and writes arriving together share one fsync |
| `persistence.directory` | `/var/lib/dzdcache` | One volume per node; `volumeClaimTemplates` in the Kubernetes manifest |

With it, a restarted node catches up from where its disk ends, and a cluster whose nodes all restart comes back with its schemas and data. Writes then also wait for the disk. The starter does not persist.

## Network and security

| Setting | Default | Choose another value when |
| --- | --- | --- |
| `cluster-token` | none, required unless standalone | Always set it: nodes refuse peers without it |
| `api-token` | empty | Server only: set it to require `Authorization: Bearer <token>` from clients |
| `cluster-tls.enabled` | `false` | Cluster traffic must be encrypted; see [cluster TLS](../dzdcache-spring-boot-starter/README.md#cluster-tls) for certificates and mutual TLS |
| `cluster-transport` | `tcp` | Only HTTP gets through between the nodes: `websocket` carries cluster traffic on the application's own web port and reached 70–110% of `tcp`'s write rate in Docker, `http2` 30–60% |
| `cluster-encoding` | `json` | `smile` encodes cluster messages in binary; it measured within noise of `json` |
| `cluster-port` | 7070 | The port is taken; with `tcp` it must be reachable between the nodes |

Every node in a cluster must use the same transport, encoding and protocol version; a node that differs is refused with the reason. With TLS on, `self-url` and `static-peers` use `https://`.

## Memory and what is kept

| Setting | Default | What it bounds |
| --- | --- | --- |
| Capacity of a type | 10,000 roots in the starter; required in a server schema | `CacheType.capacity` in the starter, `capacity` in a server schema. Every node evicts the same least recently used roots, so the copies stay equal; a `get` counts as use, a search does not |
| TTL of a type | infinite | `CacheType.ttl`, or `ttl` in a server schema |
| `max-revisions-per-schema` | 100,000 | Identities a type remembers revisions for, including evicted and invalidated ones; new identities are refused at the limit |
| `revision-retention` | 24 hours | How long records of invalidated, expired and evicted roots are kept to fence late writes |
| `changes-buffer` | 0 in the starter, 100,000 in the server | Changes kept for subscribers of the change stream that resume from an earlier position. Live listeners need none, and each kept change holds a copy of its value |
| `log-retention-bytes` | 64 MiB | The log a lagging node can catch up from; behind that, it is sent a snapshot |

Capacity counts roots, not bytes, so size it from the memory one root takes; the [benchmark](BENCHMARK.md#memory-consumption-and-measurement-limitations) measured about 1,600 bytes per node for each of its small person records in the starter.

## Limits on requests

| Setting | Default | Returns when exceeded |
| --- | --- | --- |
| `max-search-results` | 10,000 | 422 for a search without a `limit` that matches more |
| `max-scan` | 100,000 | 422 for a search with `scan` that would cover more roots |
| `max-request-bytes` | 8 MiB | 413 |
| `max-schemas` | 100 | 429 |

## Timing

The defaults fail over in about two to three seconds on a local network. Change them only when nodes are far apart or pauses are long, such as long garbage-collection pauses.

| Setting | Default | Rule |
| --- | --- | --- |
| `heartbeat-interval` | 200 milliseconds | How often the master reaches the others |
| `election-timeout` | 1 second | At least twice `heartbeat-interval`; longer avoids needless elections on a slow network, shorter fails over sooner |
| `read-lease` | 500 milliseconds | Between twice `heartbeat-interval` and `election-timeout` |
| `member-timeout` | 30 seconds | Above `election-timeout`; how long a silent learner is kept |
| `two-node-takeover` | 3 seconds | Two nodes without a witness: how long the younger waits before taking over |

## Every setting

The full lists, with every default, are in the [starter README](../dzdcache-spring-boot-starter/README.md#configuration) and the [server README](../dzdcache-server/README.md#operational-bounds).
