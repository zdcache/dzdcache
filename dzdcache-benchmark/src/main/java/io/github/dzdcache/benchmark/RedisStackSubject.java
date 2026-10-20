package io.github.dzdcache.benchmark;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.lifecycle.Startables;
import redis.clients.jedis.AbstractPipeline;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.Response;
import redis.clients.jedis.json.JsonProtocol.JsonCommand;
import redis.clients.jedis.json.Path2;
import redis.clients.jedis.search.Document;
import redis.clients.jedis.search.FTCreateParams;
import redis.clients.jedis.search.FTSearchParams;
import redis.clients.jedis.search.IndexDataType;
import redis.clients.jedis.search.schemafields.NumericField;
import redis.clients.jedis.search.schemafields.TagField;
import tools.jackson.databind.json.JsonMapper;

public final class RedisStackSubject implements DistributedSubject {
    private static final String IMAGE = "redis/redis-stack-server:7.4.0-v1";
    private static final String INDEX = "people";
    private static final int LIMIT = 1_000_000;
    private static final Pattern USED_MEMORY = Pattern.compile("used_memory:(\\d+)");

    private final JsonMapper json = JsonMapper.builder().build();
    private final Network network = Network.newNetwork();
    private final List<GenericContainer<?>> containers = new ArrayList<>();
    private final List<JedisPooled> clients = new ArrayList<>();

    public static boolean available() { return DockerClientFactory.instance().isDockerAvailable(); }

    public RedisStackSubject(int capacity) {
        try {
            containers.add(container("primary", ""));
            containers.add(container("replica-1", "--replicaof primary 6379"));
            containers.add(container("replica-2", "--replicaof primary 6379"));
            Startables.deepStart(containers).join();
            for (GenericContainer<?> container : containers) clients.add(new JedisPooled(container.getHost(), container.getMappedPort(6379)));
            Waits.until("both replicas are in sync", () -> clients.subList(1, 3).stream().allMatch(client -> info(client, "replication").contains("master_link_status:up")));
            for (JedisPooled client : clients) {
                client.ftConfigSet("MAXEXPANSIONS", String.valueOf(LIMIT));
                client.ftConfigSet("MAXSEARCHRESULTS", String.valueOf(LIMIT));
                client.ftConfigSet("TIMEOUT", "0");
            }
            primary().ftCreate(INDEX, FTCreateParams.createParams().on(IndexDataType.JSON).prefix("person:"), List.of(
                TagField.of("$.city").as("city").caseSensitive(), TagField.of("$.name").as("name").caseSensitive(),
                NumericField.of("$.age").as("age").sortable(), TagField.of("$.tags[*]").as("tags").caseSensitive()));
            Waits.until("the replica has the index", () -> {
                try { replica().ftInfo(INDEX); return true; } catch (RuntimeException e) { return false; }
            });
        } catch (RuntimeException e) { close(); throw e; }
    }

    private GenericContainer<?> container(String alias, String args) {
        return new GenericContainer<>(IMAGE).withNetwork(network).withNetworkAliases(alias).withExposedPorts(6379).withEnv("REDIS_ARGS", args);
    }

    private JedisPooled primary() { return clients.getFirst(); }
    private JedisPooled replica() { return clients.get(1); }
    private static String info(JedisPooled client, String section) {
        return new String((byte[]) client.sendCommand(Protocol.Command.INFO, section), StandardCharsets.UTF_8);
    }
    private static String key(long id) { return "person:" + id; }
    private static String escape(String value) { return value.replaceAll("([^A-Za-z0-9_])", "\\\\$1"); }

    private List<Person> search(String query) {
        List<Person> people = new ArrayList<>();
        for (Document document : replica().ftSearch(INDEX, query, FTSearchParams.searchParams().limit(0, LIMIT).dialect(2)).getDocuments())
            people.add(json.readValue(document.getString("$"), Person.class));
        return people;
    }

    private void write(Person person) {
        String reply = primary().jsonSet(key(person.id()), Path2.ROOT_PATH, json.writeValueAsString(person));
        if (!"OK".equals(reply)) throw new IllegalStateException("Write failed: " + reply);
    }

    private long indexed() { return Long.parseLong(String.valueOf(replica().ftInfo(INDEX).get("num_docs"))); }

    @Override public int nodesInThisJvm() { return 0; }
    @Override public OptionalLong externalBytes() {
        Matcher matcher = USED_MEMORY.matcher(info(replica(), "memory"));
        return matcher.find() ? OptionalLong.of(Long.parseLong(matcher.group(1))) : OptionalLong.empty();
    }

    @Override public String settings() {
        return "Redis Stack 7.4 in Docker, a primary and two asynchronous replicas; JSON documents with a RediSearch index of case-sensitive TAG city, "
            + "name (prefix query) and tags, and NUMERIC SORTABLE age; reads and FT.SEARCH run on replica 1, while put goes to the primary because replicas are read-only";
    }
    @Override public void load(Person[] people) {
        for (int start = 0; start < people.length; start += 1_000) {
            List<Response<String>> replies = new ArrayList<>();
            try (AbstractPipeline pipeline = primary().pipelined()) {
                for (int i = start; i < Math.min(people.length, start + 1_000); i++)
                    replies.add(pipeline.jsonSet(key(people[i].id()), Path2.ROOT_PATH, json.writeValueAsString(people[i])));
                pipeline.sync();
            }
            for (Response<String> reply : replies) if (!"OK".equals(reply.get())) throw new IllegalStateException("Load failed: " + reply.get());
        }
        Waits.until("the replica has indexed every person", () -> indexed() == people.length);
    }
    @Override public void putViaFirst(Person person) { write(person); }
    @Override public void put(Person person) { write(person); }
    @Override public Person get(long id) {
        Object document = replica().sendCommand(JsonCommand.GET, key(id));
        return document == null ? null : json.readValue((byte[]) document, Person.class);
    }
    @Override public List<Person> byCity(String city) { return search("@city:{" + escape(city) + "}"); }
    @Override public List<Person> byNamePrefix(String prefix) { return search("@name:{" + escape(prefix) + "*}"); }
    @Override public List<Person> byAge(int from, int to) { return search("@age:[" + from + " " + to + "]"); }
    @Override public List<Person> byCityAndAge(String city, int from, int to) {
        return search("@city:{" + escape(city) + "} @age:[" + from + " " + to + "]");
    }
    @Override public List<Person> byTag(String tag) { return search("@tags:{" + escape(tag) + "}"); }
    @Override public void close() {
        clients.forEach(JedisPooled::close);
        clients.clear();
        containers.forEach(GenericContainer::stop);
        network.close();
    }
}
