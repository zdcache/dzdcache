# DZDCache JSON root adapter feasibility proof

This standalone Java program demonstrates that ZDCache can index JSON root graphs without Student or Teacher classes. It is an experiment, not a server implementation. The small embedded contract supports string exact/prefix fields, int64 identity/range fields, and a nested array prefix field. It is deliberately not a complete metadata compiler.

Verified against ZDCache revision `5b2e759b948af905fabf2ab9827f5d765119651d`, Java 21.0.6, and Jackson 2.22.3 (annotations 2.22). Caffeine 3.2.4 was needed to compile the repository's optional adapter, but was omitted from the runtime classpath; the proof uses ZDCache's default store.

All 11 checks passed. The checks cover real behavior: root graph retention, index replacement, isolation between logical types, and cleanup under removal and LRU eviction.

For this investigation, source and downloaded dependency JARs are in `/tmp/zdcache-service-investigation-20261014`. From that directory, using a Java 21 `javac` and `java`:

```sh
mkdir -p proof-classes
rg --files src/main/java -g '*.java' > /tmp/zdcache-proof-sources.txt
javac --release 21 \
  -cp 'caffeine.jar:jackson-databind.jar:jackson-core.jar:jackson-annotations.jar' \
  -d proof-classes @/tmp/zdcache-proof-sources.txt \
  /Users/steinardragsnes/memcache/experiments/json-root-adapter/JsonRootAdapterProof.java
java -cp 'proof-classes:jackson-databind.jar:jackson-core.jar:jackson-annotations.jar' \
  JsonRootAdapterProof
```

Temporary dependencies may be cleaned by the operating system. To reproduce later, check out the stated ZDCache revision and obtain those dependency versions from Maven Central. This proof compiled the actual repository source; it does not establish that a ZDCache binary with the README's Maven coordinates has been published.
