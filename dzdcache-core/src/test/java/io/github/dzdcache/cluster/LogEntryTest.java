package io.github.dzdcache.cluster;

import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;

class LogEntryTest {
    private final JsonMapper mapper = JsonMapper.builder().build();

    @Test void theMastersMeasuredSizeTravelsWithTheEntry() {
        LogEntry draft = new LogEntry(7, 2, 1_000, new LogEntry.Noop());
        LogEntry measured = draft.withBytes(mapper.writeValueAsBytes(draft).length);
        LogEntry received = mapper.readValue(mapper.writeValueAsBytes(measured), LogEntry.class);
        assertThat(received).isEqualTo(measured);
        assertThat(received.bytes()).isEqualTo(mapper.writeValueAsBytes(draft).length).isPositive();
    }
}
