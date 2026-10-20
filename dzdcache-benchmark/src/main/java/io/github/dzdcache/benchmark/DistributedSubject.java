package io.github.dzdcache.benchmark;

import java.util.List;
import java.util.OptionalLong;

public interface DistributedSubject extends AutoCloseable {
    String settings();
    void load(Person[] people);
    void putViaFirst(Person person);
    void put(Person person);
    Person get(long id);
    List<Person> byCity(String city);
    List<Person> byNamePrefix(String prefix);
    List<Person> byAge(int from, int to);
    List<Person> byCityAndAge(String city, int from, int to);
    List<Person> byTag(String tag);
    default int nodesInThisJvm() { return 3; }
    default OptionalLong externalBytes() { return OptionalLong.empty(); }
    @Override void close();
}
