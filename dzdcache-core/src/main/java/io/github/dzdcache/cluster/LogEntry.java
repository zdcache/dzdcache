package io.github.dzdcache.cluster;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.github.dzdcache.cache.Access;
import io.github.dzdcache.cache.EntryMutation;
import io.github.dzdcache.cache.TypeDescriptor;
import java.util.List;

public record LogEntry(long index, long term, long at, Payload payload) {
    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
    @JsonSubTypes({
        @JsonSubTypes.Type(value = Publish.class, name = "publish"),
        @JsonSubTypes.Type(value = Mutations.class, name = "mutations"),
        @JsonSubTypes.Type(value = Accesses.class, name = "accesses"),
        @JsonSubTypes.Type(value = Membership.class, name = "membership"),
        @JsonSubTypes.Type(value = Sweep.class, name = "sweep"),
        @JsonSubTypes.Type(value = Noop.class, name = "noop")})
    public sealed interface Payload permits Publish, Mutations, Accesses, Membership, Sweep, Noop { }
    public record Publish(TypeDescriptor schema) implements Payload { }
    public record Mutations(List<EntryMutation> mutations) implements Payload { }
    public record Accesses(List<Access> accesses) implements Payload { }
    public record Membership(List<Member> members) implements Payload { }
    public record Sweep() implements Payload { }
    public record Noop() implements Payload { }
}
