package io.github.dzdcache.cluster;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.github.dzdcache.cache.Access;
import io.github.dzdcache.cache.EntryMutation;
import io.github.dzdcache.cache.TypeDescriptor;
import java.util.List;

public record LogEntry(long index, long term, long at, Payload payload, int bytes) {
    public LogEntry(long index, long term, long at, Payload payload) { this(index, term, at, payload, 0); }
    LogEntry withBytes(int bytes) { return new LogEntry(index, term, at, payload, bytes); }
    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
    @JsonSubTypes({
        @JsonSubTypes.Type(value = Publish.class, name = "publish"),
        @JsonSubTypes.Type(value = Mutations.class, name = "mutations"),
        @JsonSubTypes.Type(value = Accesses.class, name = "accesses"),
        @JsonSubTypes.Type(value = Membership.class, name = "membership"),
        @JsonSubTypes.Type(value = Sweep.class, name = "sweep"),
        @JsonSubTypes.Type(value = Noop.class, name = "noop"),
        @JsonSubTypes.Type(value = SpecificationChange.class, name = "specification-change"),
        @JsonSubTypes.Type(value = SpecificationCommit.class, name = "specification-commit"),
        @JsonSubTypes.Type(value = SpecificationAbandon.class, name = "specification-abandon")})
    public sealed interface Payload permits Publish, Mutations, Accesses, Membership, Sweep, Noop,
        SpecificationChange, SpecificationCommit, SpecificationAbandon { }
    public record Publish(TypeDescriptor schema) implements Payload { }
    public record Mutations(List<EntryMutation> mutations) implements Payload { }
    public record Accesses(List<Access> accesses) implements Payload { }
    public record Membership(List<Member> members, int size, List<Member> previousMembers, int previousSize, List<Member> learners) implements Payload {
        public Membership {
            learners = learners == null ? List.of() : List.copyOf(learners);
        }
        public Membership(List<Member> members, int size, List<Member> learners) { this(members, size, null, 0, learners); }
    }
    public record Sweep() implements Payload { }
    public record Noop() implements Payload { }
    public record SpecificationChange(TypeDescriptor next) implements Payload { }
    public record SpecificationCommit(String specification, int version, String fingerprint) implements Payload { }
    public record SpecificationAbandon(String specification, int version, String fingerprint, String reason) implements Payload { }
}
