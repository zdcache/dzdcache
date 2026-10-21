# Changelog

## [Unreleased]

### Breaking

- Nodes talk to each other over one TCP connection per peer with length-prefixed messages instead of an HTTP request per message, and the cluster protocol is version 2. A cluster of 0.1 nodes cannot be upgraded one node at a time; a node of 0.1 that reaches a new node is told which protocol it speaks.

### Added

- Persistence for the server: with `dzdcache.persistence.enabled`, each node keeps its log, term and vote in a journal under `dzdcache.persistence.directory`, and a write is acknowledged once it is on the disks of a majority. Writes arriving together share one fsync, and the journal is compacted into snapshots. A restarted node keeps its slot and catches up from where its disk ends; a cluster whose nodes all restart keeps its schemas and data. `k8s/dzdcache.yaml` gives each pod a volume.
- `dzdcache.cluster-transport: http2` sends cluster traffic over HTTP/2 on the application's own servlet web server (Tomcat in the server), for networks where only HTTP gets through. The default stays the framed TCP transport, which was about twice as fast in Docker.
- Learners: with `dzdcache.role: learner`, a node holds a full copy and applies the log without voting, for read replicas and near caches that do not enlarge the quorum. By default a learner holds a read lease and answers fresh; with `dzdcache.learner-fresh-reads: false` writes do not wait for it and it answers stale within `max-staleness`. `GET /cache/cluster` lists the learners, `dzdcache.max-learners` caps them, and a learner that stays silent for `member-timeout` is removed. The cluster protocol is version 3.
- `dzdcache.cluster-transport: websocket` sends cluster traffic over one WebSocket per peer on the application's own servlet web server, and `dzdcache.cluster-encoding: smile` encodes cluster messages in binary Smile, with every transport. A node whose transport or encoding differs from its peers' is refused with the reason. In Docker, writes through the WebSocket transport reached 70–110% of framed TCP, against 30–60% for HTTP/2; Smile was within measurement noise of JSON, so the defaults stay `tcp` and `json`.
- `dzdcache.max-staleness` lets a node that cannot confirm it is current answer from its own copy instead of 503, labelled as stale with its age, and stay ready while it does. The default, 0, keeps reads fresh or refused.
- Searches can be narrowed in the cache: the server's search takes a `filter` (conditions on any path, combined with `all`, `any` and `not`), `count`, `countBy` and `scan`, and the starter's `ClusterCache` takes a predicate and has `count`, `countBy` and `scan`. A scan is capped by `dzdcache.max-scan`.
- A change stream: `GET /cache/changes` in the server (Server-Sent Events, resumable with `Last-Event-ID`) and `ClusterCache.onChange` in the starter report puts, invalidations, evictions and expiries in log order, at the same positions on every node.
- Metrics for each step of a write: `dzdcache.write.duration`, `dzdcache.write.stage`, `dzdcache.cluster.round-trip`, `dzdcache.cluster.handled`, `dzdcache.cluster.loop.delay`, and with persistence `dzdcache.storage.fsync`, `dzdcache.storage.batch` and `dzdcache.storage.bytes`.

### Changed

- Followers send concurrent writes to the master together, which raises write throughput under concurrency.

### Fixed

- A node that had joined but not yet caught up could tell a member that it was not part of the cluster, and the member then left; under unlucky timing a whole cluster dissolved and a new one was founded without its schemas.
- A node whose self-url reaches another node could be made a founding member.
- A resize that removed the current master reported 503 although it had completed, and the remaining members could start an election before the change that removed the master was committed.

## [0.1.1] - 2026-10-20

### Fixed

- Writes in a cluster on Linux, which includes Docker and Kubernetes, took about 90 ms each, because the cluster port's HTTP server did not set `TCP_NODELAY`: every message between nodes waited for a delayed TCP acknowledgment. A write in a three-node cluster in Docker now takes about 10 ms. macOS was not affected.
- Acknowledging writes could fail with a `ConcurrentModificationException` when a completed write started the next step of a membership change, such as a resize. Later writes in the same batch were then left unanswered until they timed out with 503.

### Changed

- The server image `ghcr.io/zdcache/dzdcache-server` is published for both `linux/amd64` and `linux/arm64` under the same tag, so it runs natively on ARM machines such as Apple Silicon and AWS Graviton.

## [0.1.0] - 2026-10-20

First release: `dzdcache-core` and `dzdcache-spring-boot-starter` on Maven Central, and the dzdcache server as an image and an executable jar.
