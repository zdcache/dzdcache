package io.github.dzdcache.benchmark;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;

class SubjectsTest {
    static List<String> subjects() { return Subjects.names(); }

    private static Set<Long> ids(List<Person> people) { return people.stream().map(Person::id).collect(Collectors.toCollection(TreeSet::new)); }
    private static Set<Long> expected(Person[] people, Predicate<Person> match) {
        return Arrays.stream(people).filter(match).map(Person::id).collect(Collectors.toCollection(TreeSet::new));
    }

    @ParameterizedTest @MethodSource("subjects")
    void everySubjectAnswersTheSameSearchesWithTheSamePeople(String name) {
        Person[] people = Dataset.people(1_000, 42);
        Queries queries = new Queries(people, 7);
        try (DistributedSubject cache = Subjects.create(name, people.length)) {
            cache.load(people);
            for (int q = 0; q < Queries.COUNT; q++) {
                String city = queries.cities[q], prefix = queries.namePrefixes[q], tag = queries.tags[q];
                int from = queries.ageFrom[q], to = queries.ageTo[q];
                assertThat(ids(cache.byCity(city))).as(name + " city " + city).isEqualTo(expected(people, p -> p.city().equals(city)));
                assertThat(ids(cache.byNamePrefix(prefix))).as(name + " prefix " + prefix).isEqualTo(expected(people, p -> p.name().startsWith(prefix)));
                assertThat(ids(cache.byAge(from, to))).as(name + " age").isEqualTo(expected(people, p -> p.age() >= from && p.age() <= to));
                assertThat(ids(cache.byCityAndAge(city, from, to))).as(name + " city and age")
                    .isEqualTo(expected(people, p -> p.city().equals(city) && p.age() >= from && p.age() <= to));
                assertThat(ids(cache.byTag(tag))).as(name + " tag " + tag).isEqualTo(expected(people, p -> p.tags().contains(tag)));
            }
            assertThat(cache.get(people[7].id())).isEqualTo(people[7]);
            Person changed = new Person(people[7].id(), "zzzzzz", "city-99", 99, List.of("vip"));
            cache.put(changed);
            assertThat(cache.get(changed.id())).isEqualTo(changed);
            assertThat(ids(cache.byCity("city-99"))).containsExactly(changed.id());
        }
    }
}
