package io.github.dzdcache.cluster;

import java.net.URI;
import java.util.Comparator;

public record Member(String id, URI url, long startedAt) {
    public static final Comparator<Member> OLDEST_FIRST = Comparator.comparingLong(Member::startedAt).thenComparing(Member::id);

    public boolean olderThan(Member other) { return OLDEST_FIRST.compare(this, other) < 0; }
}
