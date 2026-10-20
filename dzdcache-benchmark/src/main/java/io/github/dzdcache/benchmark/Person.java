package io.github.dzdcache.benchmark;

import java.util.List;

public record Person(long id, String name, String city, int age, List<String> tags) {}
