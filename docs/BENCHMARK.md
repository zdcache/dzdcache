# Searching a distributed cache: dzdcache against Hazelcast, Ignite, Infinispan and Redis Stack

Generated 2026-10-21 (full run) on OpenJDK 64-Bit Server VM 25.0.1, 11 CPUs, everything on one machine. Every subject runs three nodes; data is loaded through node 1 and every measured call is made on node 2 by 4 threads. Operations per second, higher is better; ± is the 99.9% confidence interval, shown when there are enough measurements. Every measurement runs in 3 fresh JVMs that each start their own cluster, and measures 3 × 3 s after a warm-up of 5 × 3 s, so that the JVMs, including the dzdcache servers in Docker, have compiled their hot code before they are measured.

The subjects fall into two groups that are only compared within each group:

- **In-process:** the cache runs inside the application's JVM, and the application calls it directly. The three nodes of each cluster run in one JVM and talk over loopback networking; node 2 answers the calls. These are dzdcache-embedded, hazelcast, ignite, infinispan.
- **Out-of-process:** a separate cache service that the application reaches over the network. Each runs as three Docker containers on one Docker network, and the benchmark calls node 2 through Docker's port mapping, so both pay for a network round trip and serialization on every call. These are dzdcache-server, redis-stack.

Docker had 4 CPUs and 6.2 GB of memory for the out-of-process subjects.

## In-process caches

### Searches

#### Exact: everyone who lives in a city

| Subject | 1,000 people | 10,000 people | 50,000 people |
|---|---:|---:|---:|
| dzdcache-embedded | 4,705,270 ± 528,623 | 1,628,874 ± 106,570 | 376,959 ± 10,531 |
| hazelcast | 25,200 ± 312 | 16,867 ± 420 | 6,917 ± 502 |
| ignite | 314,259 ± 8,931 | 45,116 ± 1,793 | 9,379 ± 195 |
| infinispan | 142,721 ± 885 | 24,033 ± 1,971 | 4,287 ± 291 |

#### Prefix: names starting with two letters

| Subject | 1,000 people | 10,000 people | 50,000 people |
|---|---:|---:|---:|
| dzdcache-embedded | 5,134,435 ± 110,908 | 5,082,251 ± 688,534 | 2,195,613 ± 188,788 |
| hazelcast | 25,534 ± 2,773 | 24,661 ± 848 | 21,026 ± 716 |
| ignite | 771,571 ± 6,298 | 330,735 ± 4,097 | 97,988 ± 1,258 |
| infinispan | 120,159 ± 43,607 | 40,213 ± 2,343 | 11,666 ± 1,802 |

#### Range: ages within ten years

| Subject | 1,000 people | 10,000 people | 50,000 people |
|---|---:|---:|---:|
| dzdcache-embedded | 1,783,861 ± 130,620 | 220,833 ± 8,450 | 40,410 ± 539 |
| hazelcast | 17,874 ± 787 | 4,975 ± 458 | 1,027 ± 60 |
| ignite | 62,339 ± 2,071 | 6,508 ± 234 | 1,105 ± 61 |
| infinispan | 56,140 ± 818 | 4,434 ± 221 | 730 ± 19 |

#### Compound: a city and an age range

| Subject | 1,000 people | 10,000 people | 50,000 people |
|---|---:|---:|---:|
| dzdcache-embedded | 4,951,294 ± 849,052 | 3,895,928 ± 412,735 | 1,519,216 ± 18,239 |
| hazelcast | 26,141 ± 475 | 22,875 ± 2,625 | 16,118 ± 3,739 |
| ignite | 744,639 ± 7,467 | 199,506 ± 3,943 | 50,800 ± 3,875 |
| infinispan | 120,388 ± 2,622 | 38,843 ± 1,362 | 9,624 ± 989 |

#### Nested collection: everyone with a tag

| Subject | 1,000 people | 10,000 people | 50,000 people |
|---|---:|---:|---:|
| dzdcache-embedded | 1,443,735 ± 75,723 | 125,813 ± 4,044 | 17,852 ± 1,843 |
| hazelcast | 18,469 ± 144 | 5,546 ± 683 | 1,142 ± 109 |
| ignite | 22,745 ± 281 | 2,137 ± 115 | 338 ± 11 |
| infinispan | 56,831 ± 8,048 | 4,494 ± 119 | 776 ± 18 |

### Key-value, for comparison

dzdcache is built for the searches above. Lookups by id and writes are shown for completeness.

#### get by id

| Subject | 1,000 people | 10,000 people | 50,000 people |
|---|---:|---:|---:|
| dzdcache-embedded | 5,274,062 ± 501,030 | 5,144,737 ± 115,111 | 4,981,482 ± 95,681 |
| hazelcast | 5,977,059 ± 167,369 | 6,011,324 ± 153,120 | 5,596,778 ± 71,991 |
| ignite | 5,246,433 ± 505,529 | 4,854,223 ± 116,253 | 4,768,776 ± 126,319 |
| infinispan | 103,451,508 ± 1,582,021 | 100,832,890 ± 1,732,696 | 91,129,014 ± 1,400,051 |

#### put

| Subject | 1,000 people | 10,000 people | 50,000 people |
|---|---:|---:|---:|
| dzdcache-embedded | 6,999 ± 53 | 6,900 ± 266 | 6,553 ± 2,042 |
| hazelcast | 19,644 ± 166 | 19,767 ± 423 | 20,237 ± 190 |
| ignite | 4,224 ± 80 | 4,094 ± 142 | 4,148 ± 1,191 |
| infinispan | 12,006 ± 155 | 11,879 ± 153 | 11,825 ± 460 |

## Out-of-process caches

### Searches

#### Exact: everyone who lives in a city

| Subject | 1,000 people | 10,000 people | 50,000 people |
|---|---:|---:|---:|
| dzdcache-server | 7,892 ± 502 | 5,842 ± 69 | 2,267 ± 63 |
| redis-stack | 7,760 ± 174 | 2,595 ± 77 | 491 ± 15 |

#### Prefix: names starting with two letters

| Subject | 1,000 people | 10,000 people | 50,000 people |
|---|---:|---:|---:|
| dzdcache-server | 8,418 ± 142 | 8,016 ± 252 | 7,013 ± 145 |
| redis-stack | 8,873 ± 211 | 7,902 ± 77 | 4,422 ± 90 |

#### Range: ages within ten years

| Subject | 1,000 people | 10,000 people | 50,000 people |
|---|---:|---:|---:|
| dzdcache-server | 6,229 ± 146 | 1,740 ± 32 | 372 ± 10 |
| redis-stack | 3,411 ± 70 | 339 ± 7 | 66 ± 2 |

#### Compound: a city and an age range

| Subject | 1,000 people | 10,000 people | 50,000 people |
|---|---:|---:|---:|
| dzdcache-server | 8,360 ± 125 | 7,662 ± 305 | 6,077 ± 154 |
| redis-stack | 8,606 ± 305 | 4,534 ± 478 | 1,750 ± 25 |

#### Nested collection: everyone with a tag

| Subject | 1,000 people | 10,000 people | 50,000 people |
|---|---:|---:|---:|
| dzdcache-server | 6,254 ± 378 | 1,789 ± 62 | 372 ± 19 |
| redis-stack | 3,932 ± 78 | 373 ± 107 | 71 ± 23 |

### Key-value, for comparison

dzdcache is built for the searches above. Lookups by id and writes are shown for completeness.

#### get by id

| Subject | 1,000 people | 10,000 people | 50,000 people |
|---|---:|---:|---:|
| dzdcache-server | 8,315 ± 298 | 8,415 ± 114 | 8,311 ± 156 |
| redis-stack | 12,765 ± 333 | 12,762 ± 68 | 12,439 ± 545 |

#### put

| Subject | 1,000 people | 10,000 people | 50,000 people |
|---|---:|---:|---:|
| dzdcache-server | 2,512 ± 93 | 2,590 ± 44 | 2,602 ± 68 |
| redis-stack | 10,987 ± 82 | 10,885 ± 172 | 10,975 ± 238 |

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
| dzdcache-embedded | in-process | 50,000 | 1,521 | retained heap in this JVM divided by its 3 nodes |
| dzdcache-server | out-of-process | 50,000 | – | not measured: the nodes run in other processes |
| hazelcast | in-process | 50,000 | 533 | retained heap in this JVM divided by its 3 nodes |
| ignite | in-process | 50,000 | 622 | reported by node 2 (off-heap data region) |
| infinispan | in-process | 50,000 | 326 | retained heap in this JVM divided by its 3 nodes |
| redis-stack | out-of-process | 50,000 | 879 | reported by node 2 (used memory) |

## How each subject is set up

| Subject | Runs | Settings |
|---|---|---|
| dzdcache-embedded | in-process | 3 Spring Boot starter instances in one JVM over TCP cluster ports; exact city, prefix name, range age, compound city+age and exact-each tags; reads are local on instance 2 and wait for a read lease |
| dzdcache-server | out-of-process | 3 dzdcache server containers (dzdcache-server:benchmark) on a Docker network; JSON over HTTP/1.1 from java.net.http to server 2 through Docker's port mapping; JSON-only registration with the same dimensions |
| hazelcast | in-process | Hazelcast 5.7.0, 3 embedded members in one JVM with TCP-IP join on 127.0.0.1; IMap with backupCount=2 (sync), readBackupData=true, in-memory format OBJECT, zero-config Compact serialization of the record; indexes HASH city, SORTED name, SORTED age, SORTED compound city+age and HASH tags[any]; Predicates queries on member 2 |
| ignite | in-process | 3 Ignite 2.17 server nodes in one JVM; REPLICATED TRANSACTIONAL FULL_SYNC caches with SQL indexes on city, name, age and (city, age), tags in a second replicated PERSON_TAG(personId, tag) table indexed on tag, written in the same pessimistic transaction and joined for the tag search; SQL on node 2 runs against its local replica, Ignite's default for replicated tables |
| infinispan | in-process | Infinispan 15.2.6, 3 embedded cache managers in one JVM with JGroups TCP + TCPPING on 127.0.0.1; REPL_SYNC cache storing Java objects (an @Indexed wrapper around the record, replicated with a hand-written ProtoStream schema and MessageMarshaller); local-heap Lucene indexes keyword city, name and tags, numeric age; indexed Ickle queries on node 2 |
| redis-stack | out-of-process | Redis Stack 7.4 in Docker, a primary and two asynchronous replicas; JSON documents with a RediSearch index of case-sensitive TAG city, name (prefix query) and tags, and NUMERIC SORTABLE age; reads and FT.SEARCH run on replica 1, while put goes to the primary because replicas are read-only |
