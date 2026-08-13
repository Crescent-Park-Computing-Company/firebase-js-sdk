---
'@firebase/database': patch
'firebase': patch
---

Fix a multi-tab flush thrash in the fork's IndexedDB persistence: concurrent
tabs no longer leapfrog each other's manifest revisions (full-tree re-stages
in every tab every write window). A Web Lock per tracked root elects
that root's writer (the lock covers exactly the resource it gates — tabs
tracking disjoint roots never interact), holder liveness is proven by a
per-root localStorage heartbeat, and a queued follower steals the lock from
a suspended holder.
CAS-conflict recovery now adopts the committed manifest without reading or
decoding ranges, and conflict retries are debounced into the ordinary write
window. Destructive cleanup is guarded: empty-tree
flushes commit through the same manifest-revision CAS as any generation,
corrupt-record removal and invalidation are revision-named, and permission
evictions purge immediately in one scope-guarded transaction — so a stale or
lease-less writer can never delete a generation the current writer just
committed, and revoked data never outlives the access that produced it.
Heartbeat takeover only triggers on a present-but-stale heartbeat; storage
that throws disables the channel and degrades to page-death handoff; stamps
are holder-tokened and removed on clean release so a predecessor's stamp
never speaks for the current holder; a stolen holder settles its lock
callback before re-queueing. A tab
whose repo is deliberately interrupted (goOffline) releases its leases and
closes its write gate — liveness is not eligibility — so an online tab
persists the newest server state; resuming re-queues politely and flushes
what was pending.
