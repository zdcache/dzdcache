package io.github.dzdcache.benchmark;

import java.util.Random;

public final class Queries {
    public static final int COUNT = 16;

    public final String[] cities = new String[COUNT];
    public final String[] namePrefixes = new String[COUNT];
    public final int[] ageFrom = new int[COUNT];
    public final int[] ageTo = new int[COUNT];
    public final String[] tags = new String[COUNT];

    public Queries(Person[] people, long seed) {
        Random random = new Random(seed);
        for (int i = 0; i < COUNT; i++) {
            Person sample = people[random.nextInt(people.length)];
            cities[i] = sample.city();
            namePrefixes[i] = sample.name().substring(0, 2);
            ageFrom[i] = 18 + random.nextInt(54);
            ageTo[i] = ageFrom[i] + 9;
        }
        Random tagRandom = new Random(seed + 1);
        for (int i = 0; i < COUNT; i++) tags[i] = Dataset.TAGS.get(tagRandom.nextInt(Dataset.TAGS.size()));
    }
}
