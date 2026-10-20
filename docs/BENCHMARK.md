# Searching a distributed cache: dzdcache against Hazelcast, Ignite, Infinispan and Redis Stack

Generated 2026-10-20 (full run) on OpenJDK 64-Bit Server VM 25.0.1, 11 CPUs, everything on one machine. Every subject runs three nodes; data is loaded through node 1 and every measured call is made on node 2 by 4 threads. Operations per second, higher is better; ± is the 99.9% confidence interval, shown when there are enough measurements.

The subjects fall into two groups that are only compared within each group:

- **In-process:** the cache runs inside the application's JVM, and the application calls it directly. The three nodes of each cluster run in one JVM and talk over loopback networking; node 2 answers the calls. These are dzdcache-embedded, hazelcast, ignite, infinispan.
- **Out-of-process:** a separate cache service that the application reaches over the network. Each runs as three Docker containers on one Docker network, and the benchmark calls node 2 through Docker's port mapping, so both pay for a network round trip and serialization on every call. These are dzdcache-server, redis-stack.

Docker had 4 CPUs and 6.2 GB of memory for the out-of-process subjects.

## In-process caches

### Searches

#### Exact: everyone who lives in a city

| Subject | 1,000 people | 10,000 people | 50,000 people |
|---|---:|---:|---:|
| dzdcache-embedded | 4,793,454 ± 1,204,756 | 1,355,754 ± 601,557 | 365,471 ± 74,275 |
| hazelcast | 22,001 ± 8,459 | 14,097 ± 6,433 | 5,717 ± 1,650 |
| ignite | 224,655 ± 182,074 | 40,892 ± 16,988 | 7,100 ± 3,195 |
| infinispan | 42,288 ± 13,913 | 13,240 ± 2,348 | 3,225 ± 1,438 |

#### Prefix: names starting with two letters

| Subject | 1,000 people | 10,000 people | 50,000 people |
|---|---:|---:|---:|
| dzdcache-embedded | 4,728,975 ± 437,715 | 4,663,305 ± 1,512,563 | 2,064,595 ± 307,773 |
| hazelcast | 20,004 ± 4,907 | 19,730 ± 6,329 | 17,107 ± 5,775 |
| ignite | 235,008 ± 137,648 | 194,901 ± 189,424 | 78,720 ± 32,264 |
| infinispan | 36,432 ± 7,119 | 16,395 ± 5,094 | 5,597 ± 1,701 |

#### Range: ages within ten years

| Subject | 1,000 people | 10,000 people | 50,000 people |
|---|---:|---:|---:|
| dzdcache-embedded | 1,631,646 ± 408,354 | 186,858 ± 97,888 | 31,830 ± 22,089 |
| hazelcast | 15,307 ± 3,192 | 3,099 ± 4,890 | 863 ± 148 |
| ignite | 57,573 ± 19,798 | 4,956 ± 4,383 | 890 ± 171 |
| infinispan | 31,310 ± 5,095 | 3,839 ± 674 | 592 ± 60 |

#### Compound: a city and an age range

| Subject | 1,000 people | 10,000 people | 50,000 people |
|---|---:|---:|---:|
| dzdcache-embedded | 4,487,618 ± 821,187 | 3,394,865 ± 514,019 | 1,346,293 ± 326,455 |
| hazelcast | 18,750 ± 5,264 | 18,018 ± 8,179 | 13,030 ± 6,277 |
| ignite | 316,810 ± 396,259 | 151,058 ± 117,845 | 37,913 ± 20,207 |
| infinispan | 33,114 ± 8,858 | 14,228 ± 3,130 | 4,856 ± 1,643 |

#### Nested collection: everyone with a tag

| Subject | 1,000 people | 10,000 people | 50,000 people |
|---|---:|---:|---:|
| dzdcache-embedded | 1,087,348 ± 461,519 | 108,485 ± 68,930 | 14,586 ± 3,527 |
| hazelcast | 14,941 ± 6,534 | 4,381 ± 1,037 | 980 ± 228 |
| ignite | 19,245 ± 8,882 | 1,857 ± 584 | 308 ± 52 |
| infinispan | 27,853 ± 7,792 | 3,493 ± 510 | 644 ± 63 |

### Key-value, for comparison

dzdcache is built for the searches above. Lookups by id and writes are shown for completeness.

#### get by id

| Subject | 1,000 people | 10,000 people | 50,000 people |
|---|---:|---:|---:|
| dzdcache-embedded | 4,899,920 ± 817,967 | 4,850,184 ± 638,234 | 4,529,147 ± 729,256 |
| hazelcast | 5,763,422 ± 1,010,217 | 5,652,657 ± 1,442,939 | 4,999,113 ± 853,264 |
| ignite | 4,907,761 ± 934,380 | 4,647,595 ± 540,435 | 4,033,912 ± 419,812 |
| infinispan | 105,464,542 ± 38,403,314 | 87,261,939 ± 48,347,472 | 66,562,655 ± 17,457,337 |

#### put

| Subject | 1,000 people | 10,000 people | 50,000 people |
|---|---:|---:|---:|
| dzdcache-embedded | 2,154 ± 729 | 2,664 ± 235 | 2,317 ± 276 |
| hazelcast | 16,501 ± 6,654 | 16,084 ± 2,565 | 17,340 ± 5,455 |
| ignite | 2,427 ± 699 | 2,161 ± 272 | 2,341 ± 344 |
| infinispan | 6,591 ± 2,723 | 7,972 ± 808 | 6,049 ± 5,097 |

## Out-of-process caches

### Searches

#### Exact: everyone who lives in a city

| Subject | 1,000 people | 10,000 people | 50,000 people |
|---|---:|---:|---:|
| dzdcache-server | 2,648 ± 1,595 | 1,969 ± 649 | 1,157 ± 228 |
| redis-stack | 6,978 ± 370 | 2,597 ± 170 | 470 ± 83 |

#### Prefix: names starting with two letters

| Subject | 1,000 people | 10,000 people | 50,000 people |
|---|---:|---:|---:|
| dzdcache-server | 2,547 ± 1,260 | 2,774 ± 1,620 | 2,356 ± 1,096 |
| redis-stack | 7,918 ± 1,783 | 6,986 ± 368 | 4,211 ± 133 |

#### Range: ages within ten years

| Subject | 1,000 people | 10,000 people | 50,000 people |
|---|---:|---:|---:|
| dzdcache-server | 1,871 ± 993 | 895 ± 294 | 270 ± 21 |
| redis-stack | 3,421 ± 289 | 342 ± 48 | 60 ± 7 |

#### Compound: a city and an age range

| Subject | 1,000 people | 10,000 people | 50,000 people |
|---|---:|---:|---:|
| dzdcache-server | 2,383 ± 1,229 | 2,540 ± 1,329 | 2,334 ± 1,324 |
| redis-stack | 7,214 ± 1,649 | 4,541 ± 281 | 1,715 ± 126 |

#### Nested collection: everyone with a tag

| Subject | 1,000 people | 10,000 people | 50,000 people |
|---|---:|---:|---:|
| dzdcache-server | 2,134 ± 942 | 899 ± 262 | 261 ± 83 |
| redis-stack | 3,974 ± 135 | 396 ± 20 | 71 ± 3 |

### Key-value, for comparison

dzdcache is built for the searches above. Lookups by id and writes are shown for completeness.

#### get by id

| Subject | 1,000 people | 10,000 people | 50,000 people |
|---|---:|---:|---:|
| dzdcache-server | 2,326 ± 1,294 | 2,595 ± 1,298 | 2,812 ± 1,685 |
| redis-stack | 12,010 ± 2,984 | 12,169 ± 1,389 | 12,189 ± 890 |

#### put

| Subject | 1,000 people | 10,000 people | 50,000 people |
|---|---:|---:|---:|
| dzdcache-server | 227 ± 129 | 265 ± 274 | 281 ± 152 |
| redis-stack | 7,922 ± 10,385 | 10,318 ± 2,574 | 9,940 ± 2,710 |

## Is an acknowledged write visible on another node?

1,000 writes through node 1, each read back on node 2 as soon as it was acknowledged.

| Subject | Runs | Writes | Stale or missing on node 2 |
|---|---|---:|---:|
| dzdcache-embedded | in-process | 1000 | 0 |
| dzdcache-server | out-of-process | 1000 | 0 |
| hazelcast | in-process | 1000 | 0 |
| ignite | in-process | 1000 | 0 |
| infinispan | in-process | 1000 | 0 |
| redis-stack | out-of-process | 1000 | 0 |

## Memory per cached person, per node

| Subject | Runs | People | Bytes per person | How |
|---|---|---:|---:|---|
| dzdcache-embedded | in-process | 50,000 | 1,509 | retained heap in this JVM divided by its 3 nodes |
| dzdcache-server | out-of-process | 50,000 | – | not measured: the nodes run in other processes |
| hazelcast | in-process | 50,000 | – | com.hazelcast.core.HazelcastException: Cannot bind to a given address: /127.0.0.1. Hazelcast cannot start. Port [57864] is already in use and auto-increment is disabled. |
| ignite | in-process | 50,000 | 606 | reported by node 2 (off-heap data region) |
| infinispan | in-process | 50,000 | 359 | retained heap in this JVM divided by its 3 nodes |
| redis-stack | out-of-process | 50,000 | 880 | reported by node 2 (used memory) |

## How each subject is set up

| Subject | Runs | Settings |
|---|---|---|
| dzdcache-embedded | in-process | 3 Spring Boot starter instances in one JVM over HTTP cluster ports; exact city, prefix name, range age, compound city+age and exact-each tags; reads are local on instance 2 and wait for a read lease |
| dzdcache-server | out-of-process | 3 dzdcache server containers (dzdcache-server:benchmark) on a Docker network; JSON over HTTP/1.1 from java.net.http to server 2 through Docker's port mapping; JSON-only registration with the same dimensions |
| hazelcast | in-process | Hazelcast 5.7.0, 3 embedded members in one JVM with TCP-IP join on 127.0.0.1; IMap with backupCount=2 (sync), readBackupData=true, in-memory format OBJECT, zero-config Compact serialization of the record; indexes HASH city, SORTED name, SORTED age, SORTED compound city+age and HASH tags[any]; Predicates queries on member 2 |
| ignite | in-process | 3 Ignite 2.17 server nodes in one JVM; REPLICATED TRANSACTIONAL FULL_SYNC caches with SQL indexes on city, name, age and (city, age), tags in a second replicated PERSON_TAG(personId, tag) table indexed on tag, written in the same pessimistic transaction and joined for the tag search; SQL on node 2 runs against its local replica, Ignite's default for replicated tables |
| infinispan | in-process | Infinispan 15.2.6, 3 embedded cache managers in one JVM with JGroups TCP + TCPPING on 127.0.0.1; REPL_SYNC cache storing Java objects (an @Indexed wrapper around the record, replicated with a hand-written ProtoStream schema and MessageMarshaller); local-heap Lucene indexes keyword city, name and tags, numeric age; indexed Ickle queries on node 2 |
| redis-stack | out-of-process | Redis Stack 7.4 in Docker, a primary and two asynchronous replicas; JSON documents with a RediSearch index of case-sensitive TAG city, name (prefix query) and tags, and NUMERIC SORTABLE age; reads and FT.SEARCH run on replica 1, while put goes to the primary because replicas are read-only |
