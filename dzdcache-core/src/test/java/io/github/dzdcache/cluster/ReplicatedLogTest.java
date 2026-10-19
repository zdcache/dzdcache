package io.github.dzdcache.cluster;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ReplicatedLogTest {
    private static LogEntry entry(long index, long term) { return new LogEntry(index, term, 0, new LogEntry.Noop()); }

    @Test void appendsTruncatesAndReportsTerms() {
        ReplicatedLog log = new ReplicatedLog();
        for (long i = 1; i <= 5; i++) log.append(entry(i, i < 3 ? 1 : 2), 10);
        assertThat(log.lastIndex()).isEqualTo(5);
        assertThat(log.lastTerm()).isEqualTo(2);
        assertThat(log.term(0)).isZero();
        assertThat(log.term(2)).isEqualTo(1);
        assertThat(log.term(6)).isEqualTo(-1);
        assertThatThrownBy(() -> log.append(entry(7, 2), 10)).isInstanceOf(IllegalStateException.class);
        log.truncateFrom(4);
        assertThat(log.lastIndex()).isEqualTo(3);
        assertThat(log.bytes()).isEqualTo(30);
    }

    @Test void pagesStayWithinTheirBudgetButAlwaysCarryOneEntry() {
        ReplicatedLog log = new ReplicatedLog();
        for (long i = 1; i <= 5; i++) log.append(entry(i, 1), 10);
        assertThat(log.from(2, 25)).extracting(LogEntry::index).containsExactly(2L, 3L);
        assertThat(log.from(2, 1)).extracting(LogEntry::index).containsExactly(2L);
        assertThat(log.from(6, 100)).isEmpty();
    }

    @Test void compactionKeepsTheTailAndTheBaseTerm() {
        ReplicatedLog log = new ReplicatedLog();
        for (long i = 1; i <= 5; i++) log.append(entry(i, i), 10);
        log.compact(3, 25);
        assertThat(log.baseIndex()).isEqualTo(3);
        assertThat(log.term(3)).isEqualTo(3);
        assertThat(log.term(2)).isEqualTo(-1);
        assertThat(log.from(1, 100)).extracting(LogEntry::index).containsExactly(4L, 5L);
        log.compact(5, 0);
        assertThat(log.baseIndex()).isEqualTo(5);
        assertThat(log.lastIndex()).isEqualTo(5);
        log.reset(9, 4);
        assertThat(log.lastIndex()).isEqualTo(9);
        assertThat(log.lastTerm()).isEqualTo(4);
    }
}
