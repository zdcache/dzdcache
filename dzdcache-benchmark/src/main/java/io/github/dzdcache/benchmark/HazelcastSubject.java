package io.github.dzdcache.benchmark;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public final class HazelcastSubject implements DistributedSubject {
    private final List<HazelcastInstance> members = new ArrayList<>();
    private final IMap<Long, Person> first;
    private final IMap<Long, Person> second;

    public HazelcastSubject(int capacity) {
        String cluster = "benchmark-" + UUID.randomUUID();
        int[] ports = Ports.free(3);
        try {
            for (int port : ports) members.add(Hazelcast.newHazelcastInstance(config(cluster, port, ports)));
        } catch (RuntimeException e) {
            close();
            throw e;
        }
        Waits.until("the Hazelcast members form one cluster", () -> members.stream().allMatch(member -> member.getCluster().getMembers().size() == 3));
        first = members.get(0).getMap("people");
        second = members.get(1).getMap("people");
    }

    private static Config config(String cluster, int port, int[] ports) {
        Config config = new Config().setClusterName(cluster).setInstanceName(cluster + "-" + port)
            .setProperty("hazelcast.logging.type", "none").setProperty("hazelcast.phone.home.enabled", "false");
        config.getJetConfig().setEnabled(false);
        config.getNetworkConfig().setPort(port).setPortAutoIncrement(false).getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getAutoDetectionConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(true);
        for (int peer : ports) join.getTcpIpConfig().addMember("127.0.0.1:" + peer);
        config.addMapConfig(new MapConfig("people").setBackupCount(2).setReadBackupData(true).setInMemoryFormat(InMemoryFormat.OBJECT)
            .addIndexConfig(new IndexConfig(IndexType.HASH, "city")).addIndexConfig(new IndexConfig(IndexType.SORTED, "name"))
            .addIndexConfig(new IndexConfig(IndexType.SORTED, "age")).addIndexConfig(new IndexConfig(IndexType.SORTED, "city", "age"))
            .addIndexConfig(new IndexConfig(IndexType.HASH, "tags[any]")));
        return config;
    }

    @Override public String settings() {
        return "Hazelcast 5.7.0, 3 embedded members in one JVM with TCP-IP join on 127.0.0.1; IMap with backupCount=2 (sync), "
            + "readBackupData=true, in-memory format OBJECT, zero-config Compact serialization of the record; indexes HASH city, "
            + "SORTED name, SORTED age, SORTED compound city+age and HASH tags[any]; Predicates queries on member 2";
    }
    @Override public void load(Person[] people) {
        for (int start = 0; start < people.length; start += 1_000) {
            Map<Long, Person> batch = new HashMap<>();
            for (int i = start; i < Math.min(people.length, start + 1_000); i++) batch.put(people[i].id(), people[i]);
            first.putAll(batch);
        }
    }
    @Override public void putViaFirst(Person person) { first.set(person.id(), person); }
    @Override public void put(Person person) { second.set(person.id(), person); }
    @Override public Person get(long id) { return second.get(id); }
    @Override public List<Person> byCity(String city) { return search(Predicates.equal("city", city)); }
    @Override public List<Person> byNamePrefix(String prefix) { return search(Predicates.like("name", prefix + "%")); }
    @Override public List<Person> byAge(int from, int to) { return search(Predicates.between("age", from, to)); }
    @Override public List<Person> byCityAndAge(String city, int from, int to) {
        return search(Predicates.and(Predicates.equal("city", city), Predicates.between("age", from, to)));
    }
    @Override public List<Person> byTag(String tag) { return search(Predicates.equal("tags[any]", tag)); }

    private List<Person> search(Predicate<Long, Person> predicate) { return new ArrayList<>(second.values(predicate)); }

    @Override public void close() {
        if (!members.isEmpty()) members.get(0).getCluster().shutdown();
        members.forEach(member -> member.getLifecycleService().terminate());
        members.clear();
    }
}
