# Searching a distributed cache: dzdcache against Hazelcast, Ignite, Infinispan and Redis Stack

Generated 2026-10-23 (full run) on OpenJDK 64-Bit Server VM 25.0.1, 11 CPUs, everything on one machine. Every subject runs three nodes; data is loaded through node 1 and every measured call is made on node 2 by 4 threads. Operations per second, higher is better; ± is the 99.9% confidence interval, shown when there are enough measurements. Every measurement runs in 3 fresh JVMs that each start their own cluster, and measures 3 × 3 s after a warm-up of 5 × 3 s, so that the JVMs, including the dzdcache servers in Docker, have compiled their hot code before they are measured.

The subjects fall into two groups that are only compared within each group:

- **In-process:** the cache runs inside the application's JVM, and the application calls it directly. The three nodes of each cluster run in one JVM and talk over loopback networking; node 2 answers the calls. These are dzdcache-embedded, hazelcast, ignite, infinispan.
- **Out-of-process:** a separate cache service that the application reaches over the network. Each runs as three Docker containers on one Docker network, and the benchmark calls node 2 through Docker's port mapping, so both pay for a network round trip and serialization on every call. These are dzdcache-server, redis-stack.

Docker had 4 CPUs and 6.2 GB of memory for the out-of-process subjects.

Each chart of operations per second shows the table below it on a logarithmic scale, where equal distances are equal ratios, because the subjects differ by up to four orders of magnitude; the exact numbers and their confidence intervals are in the tables.

## In-process caches

### Searches

#### Exact: everyone who lives in a city

![Exact: everyone who lives in a city (in-process)](benchmark/in-process-exact-city.svg)

| Subject | 1,000 people | 10,000 people | 50,000 people |
|---|---:|---:|---:|
| dzdcache-embedded | 6,567,364 ± 271,333 | 1,521,328 ± 242,256 | 354,929 ± 78,515 |
| hazelcast | 24,046 ± 2,318 | 16,199 ± 562 | 6,757 ± 470 |
| ignite | 310,531 ± 9,901 | 46,368 ± 1,877 | 8,928 ± 329 |
| infinispan | 146,776 ± 554 | 24,206 ± 1,470 | 4,051 ± 222 |

#### Prefix: names starting with two letters

![Prefix: names starting with two letters (in-process)](benchmark/in-process-prefix-name.svg)

| Subject | 1,000 people | 10,000 people | 50,000 people |
|---|---:|---:|---:|
| dzdcache-embedded | 5,759,971 ± 610,612 | 6,636,151 ± 324,565 | 2,426,479 ± 82,936 |
| hazelcast | 26,565 ± 336 | 24,418 ± 344 | 20,354 ± 578 |
| ignite | 779,776 ± 10,342 | 330,266 ± 1,645 | 94,179 ± 1,371 |
| infinispan | 130,881 ± 1,577 | 39,965 ± 1,572 | 10,665 ± 1,038 |

#### Range: ages within ten years

![Range: ages within ten years (in-process)](benchmark/in-process-range-age.svg)

| Subject | 1,000 people | 10,000 people | 50,000 people |
|---|---:|---:|---:|
| dzdcache-embedded | 1,761,199 ± 115,552 | 228,066 ± 3,823 | 38,954 ± 1,513 |
| hazelcast | 17,711 ± 445 | 4,833 ± 113 | 953 ± 76 |
| ignite | 61,112 ± 2,247 | 6,516 ± 63 | 1,026 ± 25 |
| infinispan | 56,028 ± 3,897 | 4,386 ± 176 | 712 ± 31 |

#### Compound: a city and an age range

![Compound: a city and an age range (in-process)](benchmark/in-process-compound-city-age.svg)

| Subject | 1,000 people | 10,000 people | 50,000 people |
|---|---:|---:|---:|
| dzdcache-embedded | 6,819,828 ± 295,742 | 3,845,137 ± 837,196 | 1,594,283 ± 35,710 |
| hazelcast | 23,275 ± 3,784 | 22,903 ± 514 | 17,376 ± 736 |
| ignite | 727,568 ± 23,795 | 195,315 ± 2,407 | 50,743 ± 2,582 |
| infinispan | 117,990 ± 4,103 | 38,139 ± 2,020 | 9,299 ± 389 |

#### Nested collection: everyone with a tag

![Nested collection: everyone with a tag (in-process)](benchmark/in-process-nested-tag.svg)

| Subject | 1,000 people | 10,000 people | 50,000 people |
|---|---:|---:|---:|
| dzdcache-embedded | 1,433,124 ± 29,999 | 132,667 ± 8,837 | 17,345 ± 775 |
| hazelcast | 18,154 ± 520 | 5,291 ± 464 | 1,128 ± 37 |
| ignite | 22,429 ± 459 | 2,011 ± 218 | 352 ± 16 |
| infinispan | 59,775 ± 2,909 | 4,242 ± 256 | 750 ± 24 |

### Key-value, for comparison

dzdcache is built for the searches above. Lookups by id and writes are shown for completeness.

#### get by id

![get by id (in-process)](benchmark/in-process-get.svg)

| Subject | 1,000 people | 10,000 people | 50,000 people |
|---|---:|---:|---:|
| dzdcache-embedded | 7,703,413 ± 401,494 | 6,645,990 ± 1,008,220 | 7,027,320 ± 711,362 |
| hazelcast | 6,105,822 ± 322,620 | 6,126,553 ± 98,475 | 5,484,493 ± 88,490 |
| ignite | 5,171,150 ± 538,537 | 4,967,381 ± 114,098 | 4,508,078 ± 239,877 |
| infinispan | 116,168,870 ± 37,408,469 | 103,326,850 ± 408,997 | 84,216,698 ± 4,402,574 |

#### put

![put (in-process)](benchmark/in-process-put.svg)

| Subject | 1,000 people | 10,000 people | 50,000 people |
|---|---:|---:|---:|
| dzdcache-embedded | 6,859 ± 74 | 6,749 ± 170 | 7,189 ± 109 |
| hazelcast | 19,795 ± 255 | 20,019 ± 403 | 21,022 ± 1,527 |
| ignite | 4,136 ± 68 | 4,025 ± 68 | 4,120 ± 124 |
| infinispan | 11,745 ± 127 | 11,703 ± 230 | 12,080 ± 214 |

## Out-of-process caches

### Searches

#### Exact: everyone who lives in a city

![Exact: everyone who lives in a city (out-of-process)](benchmark/out-of-process-exact-city.svg)

| Subject | 1,000 people | 10,000 people | 50,000 people |
|---|---:|---:|---:|
| dzdcache-server | 7,770 ± 155 | 5,569 ± 240 | 2,246 ± 50 |
| redis-stack | 7,374 ± 186 | 2,666 ± 45 | 468 ± 16 |

#### Prefix: names starting with two letters

![Prefix: names starting with two letters (out-of-process)](benchmark/out-of-process-prefix-name.svg)

| Subject | 1,000 people | 10,000 people | 50,000 people |
|---|---:|---:|---:|
| dzdcache-server | 8,090 ± 200 | 7,845 ± 118 | 6,817 ± 133 |
| redis-stack | 8,518 ± 110 | 7,385 ± 444 | 4,082 ± 91 |

#### Range: ages within ten years

![Range: ages within ten years (out-of-process)](benchmark/out-of-process-range-age.svg)

| Subject | 1,000 people | 10,000 people | 50,000 people |
|---|---:|---:|---:|
| dzdcache-server | 5,990 ± 135 | 1,647 ± 45 | 356 ± 15 |
| redis-stack | 3,335 ± 59 | 328 ± 7 | 62 ± 2 |

#### Compound: a city and an age range

![Compound: a city and an age range (out-of-process)](benchmark/out-of-process-compound-city-age.svg)

| Subject | 1,000 people | 10,000 people | 50,000 people |
|---|---:|---:|---:|
| dzdcache-server | 7,589 ± 1,319 | 7,538 ± 107 | 5,724 ± 801 |
| redis-stack | 8,161 ± 258 | 4,307 ± 93 | 1,660 ± 26 |

#### Nested collection: everyone with a tag

![Nested collection: everyone with a tag (out-of-process)](benchmark/out-of-process-nested-tag.svg)

| Subject | 1,000 people | 10,000 people | 50,000 people |
|---|---:|---:|---:|
| dzdcache-server | 6,038 ± 161 | 1,734 ± 34 | 364 ± 20 |
| redis-stack | 3,815 ± 159 | 374 ± 7 | 68 ± 7 |

### Key-value, for comparison

dzdcache is built for the searches above. Lookups by id and writes are shown for completeness.

#### get by id

![get by id (out-of-process)](benchmark/out-of-process-get.svg)

| Subject | 1,000 people | 10,000 people | 50,000 people |
|---|---:|---:|---:|
| dzdcache-server | 8,040 ± 236 | 8,017 ± 301 | 7,985 ± 265 |
| redis-stack | 12,496 ± 266 | 12,347 ± 833 | 12,534 ± 74 |

#### put

![put (out-of-process)](benchmark/out-of-process-put.svg)

| Subject | 1,000 people | 10,000 people | 50,000 people |
|---|---:|---:|---:|
| dzdcache-server | 2,630 ± 315 | 2,583 ± 230 | 2,726 ± 216 |
| redis-stack | 10,756 ± 117 | 10,740 ± 321 | 10,713 ± 118 |

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

![Memory per cached person, per node](benchmark/memory.svg)

| Subject | Runs | People | Bytes per person | How |
|---|---|---:|---:|---|
| dzdcache-embedded | in-process | 50,000 | 1,556 | retained heap in this JVM divided by its 3 nodes |
| dzdcache-server | out-of-process | 50,000 | – | not measured: the nodes run in other processes |
| hazelcast | in-process | 50,000 | 568 | retained heap in this JVM divided by its 3 nodes |
| ignite | in-process | 50,000 | 608 | reported by node 2 (off-heap data region) |
| infinispan | in-process | 50,000 | 324 | retained heap in this JVM divided by its 3 nodes |
| redis-stack | out-of-process | 50,000 | 880 | reported by node 2 (used memory) |

## How each subject is set up

| Subject | Runs | Settings |
|---|---|---|
| dzdcache-embedded | in-process | 3 Spring Boot starter instances in one JVM over TCP cluster ports; exact city, prefix name, range age, compound city+age and exact-each tags; reads are local on instance 2 and wait for a read lease |
| dzdcache-server | out-of-process | 3 dzdcache server containers (dzdcache-server:benchmark) on a Docker network; JSON over HTTP/1.1 from java.net.http to server 2 through Docker's port mapping; JSON-only registration with the same dimensions; between the servers framed TCP on the cluster port carrying JSON |
| hazelcast | in-process | Hazelcast 5.7.0, 3 embedded members in one JVM with TCP-IP join on 127.0.0.1; IMap with backupCount=2 (sync), readBackupData=true, in-memory format OBJECT, zero-config Compact serialization of the record; indexes HASH city, SORTED name, SORTED age, SORTED compound city+age and HASH tags[any]; Predicates queries on member 2 |
| ignite | in-process | 3 Ignite 2.17 server nodes in one JVM; REPLICATED TRANSACTIONAL FULL_SYNC caches with SQL indexes on city, name, age and (city, age), tags in a second replicated PERSON_TAG(personId, tag) table indexed on tag, written in the same pessimistic transaction and joined for the tag search; SQL on node 2 runs against its local replica, Ignite's default for replicated tables |
| infinispan | in-process | Infinispan 15.2.6, 3 embedded cache managers in one JVM with JGroups TCP + TCPPING on 127.0.0.1; REPL_SYNC cache storing Java objects (an @Indexed wrapper around the record, replicated with a hand-written ProtoStream schema and MessageMarshaller); local-heap Lucene indexes keyword city, name and tags, numeric age; indexed Ickle queries on node 2 |
| redis-stack | out-of-process | Redis Stack 7.4 in Docker, a primary and two asynchronous replicas; JSON documents with a RediSearch index of case-sensitive TAG city, name (prefix query) and tags, and NUMERIC SORTABLE age; reads and FT.SEARCH run on replica 1, while put goes to the primary because replicas are read-only |
