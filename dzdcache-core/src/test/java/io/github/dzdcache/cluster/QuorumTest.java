package io.github.dzdcache.cluster;

import java.net.URI;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class QuorumTest {
    private static List<Member> members(int count) {
        return java.util.stream.IntStream.range(0, count)
            .mapToObj(i -> new Member("n" + i, URI.create("http://n" + i), 1_000 + i)).toList();
    }

    @Test void oneAndTwoMemberClustersNeedOnlyOneMember() {
        assertThat(Quorum.reached(members(1), Set.of("n0"))).isTrue();
        assertThat(Quorum.reached(members(2), Set.of("n0"))).isTrue();
        assertThat(Quorum.reached(members(2), Set.of("n1"))).isTrue();
        assertThat(Quorum.reached(members(2), Set.of())).isFalse();
    }

    @Test void oddClustersNeedAMajority() {
        assertThat(Quorum.reached(members(3), Set.of("n1", "n2"))).isTrue();
        assertThat(Quorum.reached(members(3), Set.of("n0"))).isFalse();
        assertThat(Quorum.reached(members(5), Set.of("n2", "n3", "n4"))).isTrue();
        assertThat(Quorum.reached(members(5), Set.of("n0", "n1"))).isFalse();
    }

    @Test void exactlyHalfOfAnEvenClusterCountsOnlyWithTheOldestMember() {
        assertThat(Quorum.reached(members(4), Set.of("n0", "n3"))).isTrue();
        assertThat(Quorum.reached(members(4), Set.of("n1", "n2"))).isFalse();
        assertThat(Quorum.reached(members(4), Set.of("n1", "n2", "n3"))).isTrue();
        assertThat(Quorum.reached(members(6), Set.of("n0", "n4", "n5"))).isTrue();
        assertThat(Quorum.reached(members(6), Set.of("n1", "n4", "n5"))).isFalse();
    }

    @Test void everyTwoQuorumsOfFourOrMoreShareAMember() {
        for (int size = 3; size <= 6; size++) {
            List<Member> members = members(size);
            for (int a = 0; a < 1 << size; a++) for (int b = 0; b < 1 << size; b++) {
                if (!Quorum.reached(members, ids(members, a)) || !Quorum.reached(members, ids(members, b))) continue;
                assertThat(a & b).as("size " + size + " quorums " + a + " and " + b).isNotZero();
            }
        }
    }

    @Test void idsOutsideTheMembershipDoNotCount() {
        assertThat(Quorum.reached(members(3), Set.of("n0", "stranger"))).isFalse();
    }

    private static Set<String> ids(List<Member> members, int mask) {
        java.util.Set<String> ids = new java.util.HashSet<>();
        for (int i = 0; i < members.size(); i++) if ((mask & 1 << i) != 0) ids.add(members.get(i).id());
        return ids;
    }
}
