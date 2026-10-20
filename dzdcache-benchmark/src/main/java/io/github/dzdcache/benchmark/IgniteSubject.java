package io.github.dzdcache.benchmark;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.logger.NullLogger;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.transactions.Transaction;

import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

public final class IgniteSubject implements DistributedSubject {
    private static final AtomicInteger INSTANCES = new AtomicInteger();
    private static final String PERSON = "BenchmarkPerson";
    private static final String PERSON_TAG = "BenchmarkPersonTag";
    private static final String SELECT = "select p._key, p._val from PERSON p ";

    private final List<Ignite> nodes = new ArrayList<>();
    private final Path work;

    public IgniteSubject(int capacity) {
        System.setProperty(IgniteSystemProperties.IGNITE_UPDATE_NOTIFIER, "false");
        System.setProperty(IgniteSystemProperties.IGNITE_NO_ASCII, "true");
        System.setProperty(IgniteSystemProperties.IGNITE_PERFORMANCE_SUGGESTIONS_DISABLED, "true");
        int[] discovery = Ports.free(3);
        int[] communication = Ports.free(3);
        String instance = "benchmark-" + INSTANCES.incrementAndGet();
        List<String> addresses = Arrays.stream(discovery).mapToObj(port -> "127.0.0.1:" + port).toList();
        try { work = Files.createTempDirectory("dzdcache-benchmark-ignite"); } catch (IOException e) { throw new UncheckedIOException(e); }
        try {
            for (int i = 0; i < 3; i++) {
                TcpDiscoverySpi discoverySpi = new TcpDiscoverySpi().setLocalAddress("127.0.0.1").setLocalPort(discovery[i]).setLocalPortRange(0)
                    .setIpFinder(new TcpDiscoveryVmIpFinder().setAddresses(addresses));
                TcpCommunicationSpi communicationSpi = new TcpCommunicationSpi().setLocalAddress("127.0.0.1").setLocalPort(communication[i]).setLocalPortRange(0);
                nodes.add(Ignition.start(new IgniteConfiguration().setIgniteInstanceName(instance + "-node-" + i).setLocalHost("127.0.0.1")
                    .setWorkDirectory(work.resolve("node-" + i).toString()).setDiscoverySpi(discoverySpi).setCommunicationSpi(communicationSpi)
                    .setPeerClassLoadingEnabled(false).setGridLogger(new NullLogger()).setMetricsLogFrequency(0)
                    .setClientConnectorConfiguration(null).setConnectorConfiguration(null).setFailureHandler(new StopNodeFailureHandler())
                    .setCacheConfiguration(people(), tags())));
            }
            Waits.until("the Ignite nodes have formed a cluster", () -> nodes.stream().allMatch(node -> node.cluster().nodes().size() == 3));
        } catch (RuntimeException e) { close(); throw e; }
    }

    private static <K> CacheConfiguration<K, BinaryObject> replicated(String name, QueryEntity entity) {
        return new CacheConfiguration<K, BinaryObject>(name).setCacheMode(CacheMode.REPLICATED).setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC).setSqlSchema("PUBLIC").setQueryEntities(List.of(entity));
    }

    private static CacheConfiguration<Long, BinaryObject> people() {
        LinkedHashMap<String, String> fields = new LinkedHashMap<>();
        fields.put("name", String.class.getName());
        fields.put("city", String.class.getName());
        fields.put("age", Integer.class.getName());
        return replicated("people", new QueryEntity(Long.class.getName(), PERSON).setTableName("PERSON").setFields(fields).setIndexes(List.of(
            new QueryIndex("city"), new QueryIndex("name"), new QueryIndex("age"), new QueryIndex(List.of("city", "age"), QueryIndexType.SORTED))));
    }

    private static CacheConfiguration<String, BinaryObject> tags() {
        LinkedHashMap<String, String> fields = new LinkedHashMap<>();
        fields.put("personId", Long.class.getName());
        fields.put("tag", String.class.getName());
        return replicated("person_tag", new QueryEntity(String.class.getName(), PERSON_TAG).setTableName("PERSON_TAG").setFields(fields)
            .setIndexes(List.of(new QueryIndex("tag"))));
    }

    private static IgniteCache<Long, BinaryObject> people(Ignite node) { return node.<Long, BinaryObject>cache("people").withKeepBinary(); }
    private static IgniteCache<String, BinaryObject> tags(Ignite node) { return node.<String, BinaryObject>cache("person_tag").withKeepBinary(); }
    private static String tagKey(long id, String tag) { return id + "/" + tag; }

    private static BinaryObject row(Ignite node, Person person) {
        return node.binary().builder(PERSON).setField("name", person.name()).setField("city", person.city()).setField("age", person.age())
            .setField("tags", person.tags().toArray(String[]::new)).build();
    }

    private static Map<String, BinaryObject> tagRows(Ignite node, Person person) {
        Map<String, BinaryObject> rows = new TreeMap<>();
        for (String tag : person.tags())
            rows.put(tagKey(person.id(), tag), node.binary().builder(PERSON_TAG).setField("personId", person.id()).setField("tag", tag).build());
        return rows;
    }

    private static Person person(long id, BinaryObject row) {
        return new Person(id, row.field("name"), row.field("city"), row.<Integer>field("age"), List.of(row.<String[]>field("tags")));
    }

    private static void write(Ignite node, Person person) {
        IgniteCache<Long, BinaryObject> people = people(node);
        IgniteCache<String, BinaryObject> tags = tags(node);
        try (Transaction tx = node.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            BinaryObject old = people.getAndPut(person.id(), row(node, person));
            if (old != null) for (String tag : old.<String[]>field("tags")) if (!person.tags().contains(tag)) tags.remove(tagKey(person.id(), tag));
            if (!person.tags().isEmpty()) tags.putAll(tagRows(node, person));
            tx.commit();
        }
    }

    private List<Person> query(String sql, Object... args) {
        List<Person> people = new ArrayList<>();
        for (List<?> row : people(nodes.get(1)).query(new SqlFieldsQuery(sql).setSchema("PUBLIC").setArgs(args)))
            people.add(person((Long) row.get(0), (BinaryObject) row.get(1)));
        return people;
    }

    private long count() {
        return (Long) people(nodes.get(1)).query(new SqlFieldsQuery("select count(*) from PERSON").setSchema("PUBLIC")).getAll().getFirst().getFirst();
    }

    @Override public String settings() {
        return "3 Ignite 2.17 server nodes in one JVM; REPLICATED TRANSACTIONAL FULL_SYNC caches with SQL indexes on city, name, age and (city, age), "
            + "tags in a second replicated PERSON_TAG(personId, tag) table indexed on tag, written in the same pessimistic transaction and joined "
            + "for the tag search; SQL on node 2 runs against its local replica, Ignite's default for replicated tables";
    }
    @Override public void load(Person[] people) {
        Ignite first = nodes.getFirst();
        try (IgniteDataStreamer<Long, BinaryObject> rows = first.dataStreamer("people"); IgniteDataStreamer<String, BinaryObject> tagRows = first.dataStreamer("person_tag")) {
            rows.keepBinary(true);
            tagRows.keepBinary(true);
            for (Person person : people) {
                rows.addData(person.id(), row(first, person));
                if (!person.tags().isEmpty()) tagRows.addData(tagRows(first, person));
            }
        }
        long expected = Arrays.stream(people).mapToLong(Person::id).distinct().count();
        Waits.until("node 2 has every person", () -> count() == expected);
    }
    @Override public void putViaFirst(Person person) { write(nodes.getFirst(), person); }
    @Override public void put(Person person) { write(nodes.get(1), person); }
    @Override public Person get(long id) {
        BinaryObject row = people(nodes.get(1)).get(id);
        return row == null ? null : person(id, row);
    }
    @Override public List<Person> byCity(String city) { return query(SELECT + "where p.city = ?", city); }
    @Override public List<Person> byNamePrefix(String prefix) {
        String end = prefix.substring(0, prefix.length() - 1) + (char) (prefix.charAt(prefix.length() - 1) + 1);
        return query(SELECT + "where p.name >= ? and p.name < ?", prefix, end);
    }
    @Override public List<Person> byAge(int from, int to) { return query(SELECT + "where p.age between ? and ?", from, to); }
    @Override public List<Person> byCityAndAge(String city, int from, int to) {
        return query(SELECT + "where p.city = ? and p.age between ? and ?", city, from, to);
    }
    @Override public List<Person> byTag(String tag) { return query(SELECT + "join PERSON_TAG t on p._key = t.personId where t.tag = ?", tag); }
    @Override public void close() {
        for (Ignite node : nodes.reversed()) {
            try { node.close(); } catch (RuntimeException ignored) { }
        }
        nodes.clear();
        try (Stream<Path> paths = Files.walk(work)) {
            paths.sorted(Comparator.reverseOrder()).forEach(path -> path.toFile().delete());
        } catch (IOException e) { throw new UncheckedIOException(e); }
    }
}
