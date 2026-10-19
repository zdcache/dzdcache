package io.github.dzdcache.cluster;

import java.util.Collection;
import java.util.List;
import java.util.Set;

public final class Quorum {
    private Quorum() { }

    public static boolean reached(int size, List<Member> members, Set<String> ids) {
        long present = members.stream().filter(member -> ids.contains(member.id())).count();
        if (size <= 0 || members.isEmpty()) return false;
        if (size <= 2) return present >= 1;
        if (present * 2 > size) return true;
        return present * 2 == size && ids.contains(oldest(members).id());
    }

    public static Member oldest(Collection<Member> members) {
        return members.stream().min(Member.OLDEST_FIRST).orElseThrow();
    }
}
