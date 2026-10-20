# Replication, revisions, and freshness

## Normal operation

Every node accepts writes and forwards them to the elected master. The master appends each change to a replicated log (schema publications, mutation batches, the gets that members report, membership changes and periodic sweeps). An entry commits once the master and every in-sync member have it, and a write is acknowledged once it has committed and every node that may serve reads has applied it. Members apply committed entries in log order, so every node reaches the same state, including which roots are evicted.

There is no message broker or shared data store. Kubernetes, or a static peer list, is only read while a node starts, to find the cluster; after joining, the master keeps the membership. A Kubernetes Service request itself is not a broadcast.

The cluster's size is configuration, not something nodes work out at runtime: `dzdcache.cluster-size` in Kubernetes (the StatefulSet's replicas), or the static peer list. Each node has a stable id (the pod name, or the self-url's host and port), so a restarted node takes back its own slot. Quorums are counted against the configured size, and a silent member keeps its slot until it returns; a node with an id beyond the configured size is refused.

## Fresh reads

A read that starts after a write was acknowledged, on any node, returns that write or something newer; a node that cannot confirm this answers 503. A node may serve reads only while it holds a lease:

- A follower asks the master for a lease every heartbeat and may serve reads for `read-lease`, measured from before it asked, once it has applied everything acknowledged when the lease was granted. The master grants leases only to in-sync members, and acknowledges a write only after every node holding a lease has applied it. A member that falls behind or is cut off stops getting leases and stops serving within one lease.
- The master serves reads while a quorum has answered it within `read-lease`. A new master neither serves reads nor acknowledges writes until `read-lease` (plus 12.5% for clock-rate drift) has passed since its election, which outlasts every lease the previous master could have granted.

Leases use each node's monotonic clock; they need clock rates to agree within a few percent, not synchronized clocks. Readiness reports whether a node can serve fresh reads, so traffic moves away from a node as it loses its lease; a client that keeps a direct connection gets 503 rather than older data.

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

A joining node asks a discovered peer for the master, joins through it, and catches up from the master's log, or installs a snapshot when the log no longer reaches back far enough. It becomes ready once it is a member, and serves reads once it holds a lease. A member that stops acknowledging within `replication-timeout` leaves the in-sync set, so writes continue without it, and rejoins the set after catching up. A restarted node joins as a new incarnation of its id and replaces the old one. Before founding or joining, a node checks that its own self-url reaches itself and that no other live node uses its id; otherwise it does not start and reports why in health.

When the master stops responding, members elect a new one. A quorum is more than half of the configured size, or exactly half including the oldest member; a node votes only for a candidate whose log is at least as complete as its own, so the new master has every acknowledged write. Writes wait for the new master, about two to three seconds including its waiting period, and reads pause on members whose leases run out until the new master grants new ones. A node founds a new cluster, with every node that answered as a member, only when a majority of the configured size has answered and none belongs to a cluster.

With two nodes and a witness (`dzdcache.witness-url`), the witness is the third vote. The master renews a claim at the witness every heartbeat and may serve reads and acknowledge writes only while it reaches its peer or holds that claim; the younger takes over only when the witness grants it the claim, which the witness does only after the old master's claim has run out. A broken link between the two nodes therefore leaves one master, and nothing acknowledged is lost. If the witness is down, the pair keeps working while the nodes reach each other, but the younger cannot take over from a crashed older node until the witness is back.

With two nodes and no witness, either one alone is a quorum. If the older stops answering, the younger stops serving reads within one lease and takes over after `two-node-takeover`. When two masters of the same pair meet, the one whose log reaches the higher index wins; the other rolls its state back to the last index both held, takes a new start time so it becomes the younger, and continues from that index without a full resynchronization. Token files are reread on each Kubernetes API request to support rotation.

Explicit `/cache/entries/evict` removes only the named current revision and retains its fence. It does not create a durable deletion tombstone. Use `/cache/entries/invalidate` with a newer revision for an authoritative data deletion or change boundary; eviction is a capacity/cache-management action.

## What this protocol does not guarantee

**Two nodes with a broken link and no witness.** If the link between the two nodes of a two-node cluster without a witness fails while both stay up for longer than `two-node-takeover`, both act as master: each serves reads and acknowledges writes the other does not see. When they reconnect, the writes the losing node acknowledged in that period are discarded. A node cannot tell a crashed peer from a broken link, so closing this case needs a third vote: a witness, or a third node.

**Beyond the tolerated failures.** A cluster tolerates fewer than half of its configured nodes failing at once. If more fail, nothing is persisted, so data acknowledged only by the failed nodes is lost; clusters that form separately through misconfiguration (different sizes or peer lists) merge into the one founded first, and the others' writes are discarded.

**Clock rates.** Leases assume node clocks run at rates within about 5% of each other.

Under the intended healthy-cluster workflow, an owner can invalidate with acknowledgment, commit its database transaction, publish a newer root, and then report success. If cache invalidation is required for that owner's freshness policy, a failed invalidation must prevent completion or trigger its explicit availability policy. A database transaction and cache write are not atomic; reliable owner delivery requires a durable outbox or equivalent recovery mechanism. DZDCache does not implement the owner's transaction integration.

## Authoritative searches and restarts

Searches return exactly the locally cached matching roots. `200 []` is an answer, not a miss signal. Nodes evict the same roots, but a node can differ from its peers until it has applied the latest log entries, so collection results can briefly differ between nodes. No completeness check is performed. Identity lookups return 404 when absent; unavailable caches return 503.

Schemas, data, and revision fences are all in memory. A total cluster restart loses all three. Owners must republish contracts and populate roots; old events must not be replayed out of order into a completely fresh cluster without an owner baseline. A healthy cache with a published but empty schema returns `200 []`. If that is unsuitable during initial population, gate consumer traffic externally until owner warmup completes.
