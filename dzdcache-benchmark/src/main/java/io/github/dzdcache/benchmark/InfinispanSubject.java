package io.github.dzdcache.benchmark;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.infinispan.Cache;
import org.infinispan.api.annotations.indexing.Basic;
import org.infinispan.api.annotations.indexing.Indexed;
import org.infinispan.api.annotations.indexing.Keyword;
import org.infinispan.commons.api.query.Query;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.IndexStorage;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.protostream.FileDescriptorSource;
import org.infinispan.protostream.MessageMarshaller;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;

public final class InfinispanSubject implements DistributedSubject {
    private static final String FROM = "FROM " + Entry.class.getName() + " p WHERE ";

    private final List<DefaultCacheManager> managers = new ArrayList<>();
    private final int capacity;
    private final Cache<Long, Entry> first;
    private final Cache<Long, Entry> second;

    public InfinispanSubject(int capacity) {
        this.capacity = capacity;
        String cluster = "benchmark-" + UUID.randomUUID();
        int[] ports = Ports.free(3);
        String hosts = IntStream.of(ports).mapToObj(port -> "127.0.0.1[" + port + "]").collect(Collectors.joining(","));
        try {
            for (int i = 0; i < 3; i++) managers.add(manager(cluster, i, ports[i], hosts));
            managers.forEach(manager -> manager.getCache("people"));
        } catch (RuntimeException e) {
            close();
            throw e;
        }
        Waits.until("the Infinispan nodes form one cluster", () -> managers.stream().allMatch(manager -> manager.getMembers().size() == 3));
        first = managers.get(0).getCache("people");
        second = managers.get(1).getCache("people");
    }

    private static DefaultCacheManager manager(String cluster, int node, int port, String hosts) {
        GlobalConfigurationBuilder global = GlobalConfigurationBuilder.defaultClusteredBuilder();
        global.cacheManagerName(cluster + "-" + node).serialization().addContextInitializer(new EntrySchema());
        global.transport().clusterName(cluster).nodeName("node-" + node).addProperty(JGroupsTransport.CONFIGURATION_STRING, jgroups(port, hosts));
        ConfigurationBuilder cache = new ConfigurationBuilder();
        cache.clustering().cacheMode(CacheMode.REPL_SYNC).encoding().mediaType(MediaType.APPLICATION_OBJECT_TYPE)
            .indexing().enable().storage(IndexStorage.LOCAL_HEAP).addIndexedEntity(Entry.class);
        DefaultCacheManager manager = new DefaultCacheManager(global.build(), false);
        manager.defineConfiguration("people", cache.build());
        manager.start();
        return manager;
    }

    private static String jgroups(int port, String hosts) {
        return """
            <config xmlns="urn:org:jgroups">
              <TCP bind_addr="127.0.0.1" bind_port="%d" port_range="0" diag.enabled="false" thread_naming_pattern="pl"
                   send_buf_size="640k" sock_conn_timeout="300ms" tcp_nodelay="true" bundler_type="transfer-queue"/>
              <RED/>
              <TCPPING initial_hosts="%s" port_range="0"/>
              <MERGE3 min_interval="10s" max_interval="30s"/>
              <FD_ALL3/>
              <VERIFY_SUSPECT2 timeout="1s"/>
              <pbcast.NAKACK2 use_mcast_xmit="false" xmit_interval="200ms"/>
              <UNICAST3 xmit_interval="200ms"/>
              <pbcast.STABLE desired_avg_gossip="5s" max_bytes="1M"/>
              <pbcast.GMS print_local_addr="false" join_timeout="1s"/>
              <UFC max_credits="4m" min_threshold="0.40"/>
              <MFC max_credits="4m" min_threshold="0.40"/>
              <FRAG4 frag_size="60k"/>
            </config>""".formatted(port, hosts);
    }

    @Indexed
    public static final class Entry {
        private final Person person;

        Entry(Person person) { this.person = person; }

        @Keyword(projectable = false, sortable = false) public String getName() { return person.name(); }
        @Keyword(projectable = false, sortable = false) public String getCity() { return person.city(); }
        @Basic(projectable = false, sortable = false) public int getAge() { return person.age(); }
        @Keyword(projectable = false, sortable = false) public List<String> getTags() { return person.tags(); }
    }

    static final class EntrySchema implements SerializationContextInitializer {
        @Override public String getProtoFileName() { return "benchmark-person.proto"; }
        @Override public String getProtoFile() {
            return """
                package benchmark;
                message Person {
                  optional int64 id = 1;
                  optional string name = 2;
                  optional string city = 3;
                  optional int32 age = 4;
                  repeated string tags = 5;
                }
                """;
        }
        @Override public void registerSchema(SerializationContext context) {
            context.registerProtoFiles(FileDescriptorSource.fromString(getProtoFileName(), getProtoFile()));
        }
        @Override public void registerMarshallers(SerializationContext context) { context.registerMarshaller(new EntryMarshaller()); }
    }

    static final class EntryMarshaller implements MessageMarshaller<Entry> {
        @Override public Class<Entry> getJavaClass() { return Entry.class; }
        @Override public String getTypeName() { return "benchmark.Person"; }
        @Override public Entry readFrom(ProtoStreamReader reader) throws IOException {
            return new Entry(new Person(reader.readLong("id"), reader.readString("name"), reader.readString("city"), reader.readInt("age"),
                List.copyOf(reader.readCollection("tags", new ArrayList<>(), String.class))));
        }
        @Override public void writeTo(ProtoStreamWriter writer, Entry entry) throws IOException {
            Person person = entry.person;
            writer.writeLong("id", person.id());
            writer.writeString("name", person.name());
            writer.writeString("city", person.city());
            writer.writeInt("age", person.age());
            writer.writeCollection("tags", person.tags(), String.class);
        }
    }

    @Override public String settings() {
        return "Infinispan 15.2.6, 3 embedded cache managers in one JVM with JGroups TCP + TCPPING on 127.0.0.1; REPL_SYNC cache "
            + "storing Java objects (an @Indexed wrapper around the record, replicated with a hand-written ProtoStream schema and "
            + "MessageMarshaller); local-heap Lucene indexes keyword city, name and tags, numeric age; indexed Ickle queries on node 2";
    }
    @Override public void load(Person[] people) {
        for (int start = 0; start < people.length; start += 1_000) {
            Map<Long, Entry> batch = new HashMap<>();
            for (int i = start; i < Math.min(people.length, start + 1_000); i++) batch.put(people[i].id(), new Entry(people[i]));
            first.putAll(batch);
        }
    }
    @Override public void putViaFirst(Person person) { first.put(person.id(), new Entry(person)); }
    @Override public void put(Person person) { second.put(person.id(), new Entry(person)); }
    @Override public Person get(long id) {
        Entry entry = second.get(id);
        return entry == null ? null : entry.person;
    }
    @Override public List<Person> byCity(String city) {
        return search(second.<Entry>query(FROM + "p.city = :city").setParameter("city", city));
    }
    @Override public List<Person> byNamePrefix(String prefix) {
        return search(second.<Entry>query(FROM + "p.name LIKE :prefix").setParameter("prefix", prefix + "%"));
    }
    @Override public List<Person> byAge(int from, int to) {
        return search(second.<Entry>query(FROM + "p.age BETWEEN :from AND :to").setParameter("from", from).setParameter("to", to));
    }
    @Override public List<Person> byCityAndAge(String city, int from, int to) {
        return search(second.<Entry>query(FROM + "p.city = :city AND p.age BETWEEN :from AND :to").setParameter("city", city)
            .setParameter("from", from).setParameter("to", to));
    }
    @Override public List<Person> byTag(String tag) { return search(second.<Entry>query(FROM + "p.tags = :tag").setParameter("tag", tag)); }

    private List<Person> search(Query<Entry> query) {
        List<Entry> entries = query.maxResults(capacity).list();
        List<Person> people = new ArrayList<>(entries.size());
        for (Entry entry : entries) people.add(entry.person);
        return people;
    }
    @Override public void close() {
        for (int i = managers.size() - 1; i >= 0; i--) managers.get(i).stop();
        managers.clear();
    }
}
