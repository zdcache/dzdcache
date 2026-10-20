package io.github.dzdcache.benchmark;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public final class Dataset {
    public static final int CITIES = 50;
    public static final List<String> TAGS =
        List.of("home", "work", "vip", "new", "churn", "b2b", "b2c", "north", "south", "west");

    private Dataset() { }

    public static String city(int i) {
        return "city-" + (i < 10 ? "0" : "") + i;
    }

    public static Person[] people(int size, long seed) {
        Random random = new Random(seed);
        Person[] people = new Person[size];
        for (int i = 0; i < size; i++) people[i] = person(i, random);
        return people;
    }

    public static Person[] changed(Person[] people, long seed) {
        Random random = new Random(seed);
        Person[] changed = new Person[people.length];
        for (int i = 0; i < people.length; i++) changed[i] = person(people[i].id(), random);
        return changed;
    }

    public static Person[] retagged(Person[] people, long seed) {
        Random random = new Random(seed);
        Person[] retagged = new Person[people.length];
        for (int i = 0; i < people.length; i++) {
            Person person = people[i];
            retagged[i] = new Person(person.id(), person.name(), person.city(), person.age(), tags(random));
        }
        return retagged;
    }

    public static int[] accessOrder(int size, int length, long seed) {
        Random random = new Random(seed);
        int[] order = new int[length];
        for (int i = 0; i < length; i++) order[i] = random.nextInt(size);
        return order;
    }

    private static Person person(long id, Random random) {
        char[] name = new char[6];
        for (int c = 0; c < name.length; c++) name[c] = (char) ('a' + random.nextInt(26));
        List<String> tags = tags(random);
        return new Person(id, new String(name), city(random.nextInt(CITIES)), 18 + random.nextInt(63), tags);
    }

    private static List<String> tags(Random random) {
        List<String> tags = new ArrayList<>();
        for (int t = random.nextInt(4); t > 0; t--) tags.add(TAGS.get(random.nextInt(TAGS.size())));
        return List.copyOf(tags);
    }
}
