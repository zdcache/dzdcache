package io.github.dzdcache.cluster;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.github.dzdcache.cache.Access;
import io.github.dzdcache.cache.EntryMutation;
import io.github.dzdcache.cache.PutRequest;
import io.github.dzdcache.cache.SchemaView;
import io.github.dzdcache.cache.SnapshotRecord;
import io.github.dzdcache.cache.TypeDescriptor;
import java.util.List;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(value = Message.Append.class, name = "append"),
    @JsonSubTypes.Type(value = Message.Appended.class, name = "appended"),
    @JsonSubTypes.Type(value = Message.Vote.class, name = "vote"),
    @JsonSubTypes.Type(value = Message.Voted.class, name = "voted"),
    @JsonSubTypes.Type(value = Message.SnapshotOffer.class, name = "snapshot-offer"),
    @JsonSubTypes.Type(value = Message.SnapshotPageRequest.class, name = "snapshot-page-request"),
    @JsonSubTypes.Type(value = Message.SnapshotPage.class, name = "snapshot-page"),
    @JsonSubTypes.Type(value = Message.WhoLeads.class, name = "who-leads"),
    @JsonSubTypes.Type(value = Message.Leader.class, name = "leader"),
    @JsonSubTypes.Type(value = Message.Join.class, name = "join"),
    @JsonSubTypes.Type(value = Message.Leave.class, name = "leave"),
    @JsonSubTypes.Type(value = Message.ForwardPublish.class, name = "forward-publish"),
    @JsonSubTypes.Type(value = Message.ForwardPuts.class, name = "forward-puts"),
    @JsonSubTypes.Type(value = Message.ForwardChange.class, name = "forward-change"),
    @JsonSubTypes.Type(value = Message.Written.class, name = "written"),
    @JsonSubTypes.Type(value = Message.AccessReport.class, name = "access-report"),
    @JsonSubTypes.Type(value = Message.LeaseRequest.class, name = "lease-request"),
    @JsonSubTypes.Type(value = Message.Lease.class, name = "lease"),
    @JsonSubTypes.Type(value = Message.Ack.class, name = "ack"),
    @JsonSubTypes.Type(value = Message.Refused.class, name = "refused")})
public sealed interface Message {
    record Append(long term, Member master, long previousIndex, long previousTerm, List<LogEntry> entries, long commitIndex,
                  List<String> members, boolean inSync, String clusterId, long lastIndex) implements Message { }
    record Appended(long term, boolean success, long matchIndex, long lastIndex, boolean snapshotNeeded, boolean installing,
                    boolean mustYield, List<String> members, long appliedIndex) implements Message { }
    record Vote(long term, Member candidate, long lastIndex, long lastTerm, List<String> members) implements Message { }
    record Voted(long term, boolean granted, boolean candidateIsMember, String clusterId) implements Message { }
    record SnapshotOffer(long term, Member master, String snapshotId, long index, long indexTerm, List<Member> members,
                         List<TypeDescriptor> schemas, String clusterId) implements Message { }
    record SnapshotPageRequest(String snapshotId, int from) implements Message { }
    record SnapshotPage(boolean missing, List<SnapshotRecord> records, Integer next) implements Message { }
    record WhoLeads() implements Message { }
    record Leader(Member self, boolean member, Member master, List<Member> members, long lastIndex, long lastTerm,
                  String clusterId, int clusterSize) implements Message { }
    record Join(Member member, int clusterSize) implements Message { }
    record Leave(Member member) implements Message { }
    record ForwardPublish(TypeDescriptor schema) implements Message { }
    record ForwardPuts(List<PutRequest> puts) implements Message { }
    record ForwardChange(EntryMutation mutation) implements Message { }
    record Written(SchemaView view, List<ClusterCoordinator.Outcome> outcomes) implements Message { }
    record AccessReport(List<Access> accesses) implements Message { }
    record LeaseRequest(long term, Member member, long appliedIndex) implements Message { }
    record Lease(long term, long millis, long ackedIndex) implements Message { }
    record Ack() implements Message { }
    record Refused(int status, String message) implements Message { }
}
