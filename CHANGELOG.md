# Changelog

## [0.1.1] - 2026-10-20

### Fixed

- Writes in a cluster on Linux, which includes Docker and Kubernetes, took about 90 ms each, because the cluster port's HTTP server did not set `TCP_NODELAY`: every message between nodes waited for a delayed TCP acknowledgment. A write in a three-node cluster in Docker now takes about 10 ms. macOS was not affected.
- Acknowledging writes could fail with a `ConcurrentModificationException` when a completed write started the next step of a membership change, such as a resize. Later writes in the same batch were then left unanswered until they timed out with 503.

### Changed

- The server image `ghcr.io/zdcache/dzdcache-server` is published for both `linux/amd64` and `linux/arm64` under the same tag, so it runs natively on ARM machines such as Apple Silicon and AWS Graviton.

## [0.1.0] - 2026-10-20

First release: `dzdcache-core` and `dzdcache-spring-boot-starter` on Maven Central, and the dzdcache server as an image and an executable jar.
