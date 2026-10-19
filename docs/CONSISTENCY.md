# Replication, revisions, and freshness

## Normal operation

Every node accepts writes and forwards them to the elected master. The master appends each change to a replicated log (schema publications, mutation batches, the gets that members report, membership changes and periodic sweeps) and acknowledges a write once it and every in-sync member have the entry. Members apply committed entries in log order, so every node reaches the same state, including which roots are evicted.

There is no message broker or shared data store. Kubernetes, or a static peer list, is only read while a node starts, to find the cluster; after joining, the master keeps the membership. A Kubernetes Service request itself is not a broadcast.

## Owner revisions

Each `(specification, schemaVersion, identity)` has a positive, monotonically increasing revision allocated by the **data owner**. Prefer a database version or durable per-entity event sequence. Revisions must order cache mutations, including invalidations; a node-local counter or wall clock alone is insufficient for concurrent owners.

- A write without a revision (or with 0) takes the identity's next revision when its log entry is applied, so the last write in log order wins; a retry of such a write is a new write. Evict without a revision removes the current revision.
- Higher revision replaces previous knowledge.
- Lower revision is ignored; successful delivery is not proof that the payload became current.
- Equal revision plus equivalent operation/payload/deadline is idempotent.
- Equal revision with conflicting content returns 409 and leaves the stored entry unchanged.
- Invalidation retains a tombstone for `revision-retention`. A delayed older put cannot resurrect the value within that time.
- LRU and TTL removal retain revision metadata. Every node reaches the same LRU decision because it applies the same writes and reported gets in the same order.

For example, invalidate revision 20 before an owner update, then publish the committed value as revision 21. Reusing revision 20 for both operations is a conflict. If the database update fails after invalidation, restore the old graph only with a newer mutation revision. Persist revision allocation with the owner's change workflow.

Consumers own data-source selection and fallback. DZDCache does not fetch business data or configure a backfill policy. A consumer that chooses to publish a fetched result also acts as a publisher: its write must carry an owner-issued revision and respect the same ordering/deadline rules. Do not invent a new revision for old data. These responsibilities apply whether applications call the HTTP API directly or through a client library.

## Failure and recovery

A mutation returning 503 can be partially applied: the entry may still commit after the response. There is no rollback. Retry exactly the same revision, graph, and absolute deadline (or original `writtenAt`).

A joining node asks a discovered peer for the master, joins through it, and catches up from the master's log, or installs a snapshot when the log no longer reaches back far enough. It becomes ready once it is a member. A member that stops acknowledging within `replication-timeout` leaves the in-sync set, so writes continue without it, and rejoins the set after catching up. A member silent for `member-timeout` is removed while at least three members remain. A restarted node joins as a new incarnation of its id and replaces the old one.

When the master stops responding, members elect a new one. A quorum is more than half of the members, or exactly half including the oldest member; a node votes only for a candidate whose log is at least as complete as its own, so the new master has every acknowledged write. Writes wait for the new master, about one to two seconds; reads continue on every member. A node founds a new cluster only when every discovered peer answers and none belongs to a cluster. Token files are reread on each Kubernetes API request to support rotation.

Explicit `/cache/entries/evict` removes only the named current revision and retains its fence. It does not create a durable deletion tombstone. Use `/cache/entries/invalidate` with a newer revision for an authoritative data deletion or change boundary; eviction is a capacity/cache-management action.

## What this protocol does not guarantee

**This is not a linearizable cache.** Members serve reads from the entries they have applied, which can lag the master; an isolated member keeps serving older data while it is cut off. With two members, a failed link lets both act as master; when they reconnect, the older node's history wins and the younger's writes from that period are discarded. Separate clusters formed during a partition merge into the one founded first, and the others' writes are discarded. Readiness removal also propagates asynchronously through Kubernetes and clients can retain direct connections.

Consequently, acknowledgment by the master's in-sync members is not proof that every possible reader has crossed the update boundary. The earlier design goal, “no stale read after the owner reports completion,” is only approached during healthy, stable membership; it is **not an unconditional guarantee of this implementation**, and no fixed millisecond staleness bound is promised. Strict enforcement would require a stronger membership/fencing protocol or authoritative read validation, with explicit partition behavior. Adding broadcast retries cannot establish that guarantee.

Under the intended healthy-cluster workflow, an owner can invalidate with acknowledgment, commit its database transaction, publish a newer root, and then report success. If cache invalidation is required for that owner's freshness policy, a failed invalidation must prevent completion or trigger its explicit availability policy. A database transaction and cache write are not atomic; reliable owner delivery requires a durable outbox or equivalent recovery mechanism. DZDCache does not implement the owner's transaction integration.

## Authoritative searches and restarts

Searches return exactly the locally cached matching roots. `200 []` is an answer, not a miss signal. Nodes evict the same roots, but a node can differ from its peers until it has applied the latest log entries, so collection results can briefly differ between nodes. No completeness check is performed. Identity lookups return 404 when absent; unavailable caches return 503.

Schemas, data, and revision fences are all in memory. A total cluster restart loses all three. Owners must republish contracts and populate roots; old events must not be replayed out of order into a completely fresh cluster without an owner baseline. A healthy cache with a published but empty schema returns `200 []`. If that is unsuitable during initial population, gate consumer traffic externally until owner warmup completes.
